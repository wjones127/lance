// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Reclaimable scratch storage.
//!
//! A [`SpillStore`] hands out [`SpillFile`] handles for temporary state that is
//! too large to keep in memory and is read back later in the same process (for
//! example, posting lists or shuffle runs accumulated while building an index).
//! The backing storage is reclaimed automatically when a handle is dropped.
//!
//! Key properties:
//! - **`writer()`/`reader()` interface.** A [`SpillFile`] vends a [`Writer`]
//!   and a [`Reader`] rather than exposing an [`ObjectStore`] and a path. The
//!   writer feeds `FileWriter::try_new` directly; the reader feeds a v2
//!   `FileReader` via [`crate::scheduler::ScanScheduler::open_reader`].
//! - **Per-file RAII.** One [`SpillFile`] is one file; dropping it deletes the
//!   file and releases its bytes back to the store's disk budget, so a caller
//!   can hold N files and reclaim them individually.
//! - **Disk cap enforcement.** [`LocalSpillStore::with_cap`] enforces a byte
//!   budget shared across all handles, returning a typed
//!   [`lance_core::Error::DiskCapExceeded`] rather than silently filling the
//!   disk.
//!
//! # Usage contract
//!
//! Hold the [`SpillFile`] alive for the file's lifetime: the [`Writer`] and
//! [`Reader`] reference the backing file, which the handle deletes on drop. The
//! store's temp directory is the backstop for anything leaked when it drops.
//!
//! Accounting is reserve-on-write + release-on-drop (by stat), which is exact
//! for the write-once contract this enforces: [`SpillFile::writer`] may be
//! called only once, so the bytes reserved while writing match the file size
//! released on drop. Two minor inexactnesses are not engineered around: a write
//! aborted at the cap leaks its reservation until the store is dropped, and a
//! file whose size cannot be stat-ed on drop is not released.

use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_trait::async_trait;
use object_store::path::Path;
use tokio::io::AsyncWrite;

use lance_core::{Error, Result};

use crate::object_store::ObjectStore;
use crate::object_writer::WriteResult;
use crate::traits::{Reader, Writer};

/// A handle to a single unit of reclaimable scratch storage.
///
/// Data is written through [`SpillFile::writer`] and read back through
/// [`SpillFile::reader`]. The backing storage is released when the handle is
/// dropped.
///
/// The trait is object-safe so it can be returned as `Box<dyn SpillFile>` from
/// [`SpillStore::create_spill_file`], allowing implementations not backed by a
/// local file (e.g. in-memory buffers, remote object stores).
#[async_trait]
pub trait SpillFile: Send + Sync {
    /// Open a writer over this spill file.
    ///
    /// For a capped store, writes that would exceed the cap fail with
    /// [`lance_core::Error::DiskCapExceeded`]. Spill files are write-once:
    /// implementations may reject a second call (the [`LocalSpillStore`] impl
    /// returns [`lance_core::Error::invalid_input`]).
    async fn writer(&self) -> Result<Box<dyn Writer>>;

    /// Open a reader over this spill file.
    ///
    /// The data must have been fully written (the writer shut down) first.
    async fn reader(&self) -> Result<Box<dyn Reader>>;
}

/// A factory for [`SpillFile`] handles.
///
/// The trait is object-safe and `Send + Sync` so it can be held behind an
/// `Arc<dyn SpillStore>` (e.g. inside a `Session`).
pub trait SpillStore: Send + Sync + 'static {
    /// Allocate a new scratch handle.
    ///
    /// The backing storage is reclaimed when the returned [`SpillFile`] is
    /// dropped.
    fn create_spill_file(&self) -> Result<Box<dyn SpillFile>>;
}

/// A shared, cloneable byte budget.
///
/// Cloning produces another handle to the *same* underlying counter, so a
/// quota shared across many writers enforces a single combined cap.
#[derive(Debug, Clone)]
struct DiskQuota {
    cap_bytes: u64,
    used: Arc<Mutex<u64>>,
}

impl DiskQuota {
    fn new(cap_bytes: u64) -> Self {
        Self {
            cap_bytes,
            used: Arc::new(Mutex::new(0)),
        }
    }

    /// Try to reserve `n` bytes, failing with [`Error::DiskCapExceeded`] if the
    /// reservation would push total usage past the cap.
    fn try_reserve(&self, n: u64) -> Result<()> {
        // The lock is held only for a couple of arithmetic ops and never across
        // an `.await`, so a std `Mutex` is the simplest correct choice.
        let mut used = self.used.lock().unwrap();
        let next = used.saturating_add(n);
        if next > self.cap_bytes {
            return Err(Error::disk_cap_exceeded(self.cap_bytes, *used));
        }
        *used = next;
        Ok(())
    }

    /// Release `n` previously reserved bytes back to the budget.
    fn release(&self, n: u64) {
        // Saturating sub keeps a stray double-release from underflowing.
        let mut used = self.used.lock().unwrap();
        *used = used.saturating_sub(n);
    }
}

/// A [`Writer`] decorator that reserves a [`DiskQuota`] as bytes are written.
///
/// Wrapping the writer keeps cap enforcement inside the spill store rather than
/// pushing it into [`ObjectStore`], and works for any backend the store opens.
struct QuotaWriter {
    inner: Box<dyn Writer>,
    quota: DiskQuota,
}

impl AsyncWrite for QuotaWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        // Reserve up-front for the bytes we intend to write, then release the
        // remainder the inner writer did not accept so the reservation tracks
        // bytes actually buffered (and, for a write-once file, the file size).
        if let Err(e) = this.quota.try_reserve(buf.len() as u64) {
            return Poll::Ready(Err(io::Error::other(e)));
        }
        let poll = Pin::new(this.inner.as_mut()).poll_write(cx, buf);
        match &poll {
            Poll::Ready(Ok(n)) => this.quota.release((buf.len() - *n) as u64),
            _ => this.quota.release(buf.len() as u64),
        }
        poll
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().inner.as_mut()).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().inner.as_mut()).poll_shutdown(cx)
    }
}

#[async_trait]
impl Writer for QuotaWriter {
    async fn tell(&mut self) -> Result<usize> {
        self.inner.tell().await
    }

    async fn shutdown(&mut self) -> Result<WriteResult> {
        self.inner.shutdown().await
    }
}

/// A [`SpillStore`] that writes temporary files to a local temp directory.
///
/// By default there is no disk cap. Use [`LocalSpillStore::with_cap`] to
/// configure one shared across every handle this store produces.
///
/// The temp directory is deleted when the store is dropped, cleaning up any
/// files whose handles have already been dropped.
pub struct LocalSpillStore {
    store: Arc<ObjectStore>,
    /// Backstop cleanup: removes the whole scratch directory on drop.
    temp_dir: Arc<tempfile::TempDir>,
    file_counter: Arc<AtomicU64>,
    /// Byte budget shared across every handle, enforced while writing.
    quota: Option<DiskQuota>,
}

impl LocalSpillStore {
    /// Create a store with no disk cap.
    pub fn new() -> Result<Self> {
        Ok(Self {
            store: Arc::new(ObjectStore::local()),
            temp_dir: Arc::new(tempfile::tempdir()?),
            file_counter: Arc::new(AtomicU64::new(0)),
            quota: None,
        })
    }

    /// Create a store that returns [`lance_core::Error::DiskCapExceeded`] once
    /// total bytes written across all live handles would exceed `cap_bytes`.
    pub fn with_cap(cap_bytes: u64) -> Result<Self> {
        Ok(Self {
            store: Arc::new(ObjectStore::local()),
            temp_dir: Arc::new(tempfile::tempdir()?),
            file_counter: Arc::new(AtomicU64::new(0)),
            quota: Some(DiskQuota::new(cap_bytes)),
        })
    }
}

impl Default for LocalSpillStore {
    fn default() -> Self {
        Self::new().expect("failed to create temp directory for LocalSpillStore")
    }
}

impl SpillStore for LocalSpillStore {
    fn create_spill_file(&self) -> Result<Box<dyn SpillFile>> {
        let idx = self.file_counter.fetch_add(1, Ordering::Relaxed);
        let fs_path = self.temp_dir.path().join(format!("spill_{idx:06}.bin"));
        let os_path = Path::from_absolute_path(&fs_path)?;
        Ok(Box::new(LocalSpillFile {
            store: self.store.clone(),
            os_path,
            fs_path,
            quota: self.quota.clone(),
            writer_taken: AtomicBool::new(false),
            _temp_dir: self.temp_dir.clone(),
        }))
    }
}

/// A [`SpillFile`] backed by a single file in the store's temp directory.
struct LocalSpillFile {
    store: Arc<ObjectStore>,
    os_path: Path,
    fs_path: PathBuf,
    quota: Option<DiskQuota>,
    /// Set once a writer has been vended, to reject the write-once violation of
    /// reopening a writer (which would truncate the file and leak the first
    /// write's reservation against the budget).
    writer_taken: AtomicBool,
    /// Keep the store's temp directory alive for at least this file's lifetime.
    _temp_dir: Arc<tempfile::TempDir>,
}

#[async_trait]
impl SpillFile for LocalSpillFile {
    async fn writer(&self) -> Result<Box<dyn Writer>> {
        if self.writer_taken.swap(true, Ordering::Relaxed) {
            return Err(Error::invalid_input(
                "spill files are write-once; this file already has a writer",
            ));
        }
        let writer = self.store.create(&self.os_path).await?;
        match &self.quota {
            Some(quota) => Ok(Box::new(QuotaWriter {
                inner: writer,
                quota: quota.clone(),
            })),
            None => Ok(writer),
        }
    }

    async fn reader(&self) -> Result<Box<dyn Reader>> {
        self.store.open(&self.os_path).await
    }
}

impl Drop for LocalSpillFile {
    fn drop(&mut self) {
        // Release the bytes this file occupied back to the budget. We stat the
        // persisted file rather than tracking writes, which is exact for the
        // write-once contract.
        if let Some(quota) = &self.quota
            && let Ok(metadata) = std::fs::metadata(&self.fs_path)
        {
            quota.release(metadata.len());
        }
        // Best-effort removal; the temp dir is the backstop.
        let _ = std::fs::remove_file(&self.fs_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    async fn write_spill(spill: &dyn SpillFile, data: &[u8]) -> Result<()> {
        let mut writer = spill.writer().await?;
        writer.write_all(data).await?;
        Writer::shutdown(writer.as_mut()).await?;
        Ok(())
    }

    #[test]
    fn test_disk_quota_reserve_release() {
        let quota = DiskQuota::new(100);
        quota.try_reserve(60).unwrap();
        assert!(quota.try_reserve(60).is_err());
        quota.release(60);
        quota.try_reserve(60).unwrap();
    }

    #[tokio::test]
    async fn test_write_then_read() {
        let store = LocalSpillStore::new().unwrap();
        let spill = store.create_spill_file().unwrap();

        let data = b"hello spill world";
        write_spill(spill.as_ref(), data).await.unwrap();

        let reader = spill.reader().await.unwrap();
        let read_back = reader.get_all().await.unwrap();
        assert_eq!(read_back.as_ref(), data);
    }

    #[tokio::test]
    async fn test_raii_cleanup() {
        let store = LocalSpillStore::new().unwrap();
        let spill = store.create_spill_file().unwrap();
        write_spill(spill.as_ref(), b"some bytes").await.unwrap();

        // The first file gets a deterministic name under the store's temp dir.
        let path = store.temp_dir.path().join("spill_000000.bin");
        assert!(path.exists());
        drop(spill);
        assert!(!path.exists(), "spill file should be deleted on drop");
    }

    #[tokio::test]
    async fn test_cap_exceeded() {
        let store = LocalSpillStore::with_cap(100).unwrap();
        let spill = store.create_spill_file().unwrap();
        let err = write_spill(spill.as_ref(), &[0u8; 101]).await.unwrap_err();
        assert!(
            matches!(err, Error::DiskCapExceeded { cap_bytes: 100, .. }),
            "expected DiskCapExceeded, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_cap_shared_across_files() {
        let store = LocalSpillStore::with_cap(100).unwrap();
        let a = store.create_spill_file().unwrap();
        let b = store.create_spill_file().unwrap();

        write_spill(a.as_ref(), &[0u8; 60]).await.unwrap();
        // 60 already reserved by `a`; writing 60 more would reach 120 > 100.
        let err = write_spill(b.as_ref(), &[0u8; 60]).await.unwrap_err();
        assert!(
            matches!(err, Error::DiskCapExceeded { cap_bytes: 100, .. }),
            "expected DiskCapExceeded, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_cap_freed_on_drop() {
        let store = LocalSpillStore::with_cap(100).unwrap();

        {
            let a = store.create_spill_file().unwrap();
            write_spill(a.as_ref(), &[0u8; 80]).await.unwrap();
            // `a` drops here, releasing its 80 bytes.
        }

        let b = store.create_spill_file().unwrap();
        // Succeeds because the cap is no longer under pressure.
        write_spill(b.as_ref(), &[0u8; 80]).await.unwrap();
    }

    #[tokio::test]
    async fn test_custom_implementation() {
        // A custom store can satisfy the traits without a local file.
        struct MemStore;
        struct MemFile;

        #[async_trait]
        impl SpillFile for MemFile {
            async fn writer(&self) -> Result<Box<dyn Writer>> {
                ObjectStore::memory().create(&Path::from("/mem")).await
            }
            async fn reader(&self) -> Result<Box<dyn Reader>> {
                ObjectStore::memory().open(&Path::from("/mem")).await
            }
        }

        impl SpillStore for MemStore {
            fn create_spill_file(&self) -> Result<Box<dyn SpillFile>> {
                Ok(Box::new(MemFile))
            }
        }

        let store = MemStore;
        let spill = store.create_spill_file().unwrap();
        // Exercise the factory + trait object; the in-memory store is a fresh
        // instance per call so we don't round-trip data here.
        let _ = spill.writer().await.unwrap();
    }

    #[tokio::test]
    async fn test_writer_is_write_once() {
        let store = LocalSpillStore::new().unwrap();
        let spill = store.create_spill_file().unwrap();

        let _writer = spill.writer().await.unwrap();
        let Err(err) = spill.writer().await else {
            panic!("second writer() should be rejected");
        };
        assert!(
            matches!(err, Error::InvalidInput { .. }),
            "second writer() should be rejected with InvalidInput, got {err:?}"
        );
    }

    /// A [`Writer`] whose `poll_write` accepts a fixed number of bytes per call,
    /// or fails, so we can drive the [`QuotaWriter`] release arms that the local
    /// backend (which accepts every write in full) never hits.
    struct ControlledWriter {
        outcome: Poll<io::Result<usize>>,
    }

    impl AsyncWrite for ControlledWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            match &self.outcome {
                Poll::Ready(Ok(n)) => Poll::Ready(Ok((*n).min(buf.len()))),
                Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::new(e.kind(), e.to_string()))),
                Poll::Pending => Poll::Pending,
            }
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[async_trait]
    impl Writer for ControlledWriter {
        async fn tell(&mut self) -> Result<usize> {
            Ok(0)
        }
        async fn shutdown(&mut self) -> Result<WriteResult> {
            Ok(WriteResult::default())
        }
    }

    #[tokio::test]
    async fn test_quota_writer_releases_unaccepted_bytes() {
        // Short write: the inner writer accepts only 10 of the 40 reserved bytes,
        // so the 30-byte remainder must be returned to the budget.
        let quota = DiskQuota::new(100);
        let mut writer = QuotaWriter {
            inner: Box::new(ControlledWriter {
                outcome: Poll::Ready(Ok(10)),
            }),
            quota: quota.clone(),
        };
        let n = writer.write(&[0u8; 40]).await.unwrap();
        assert_eq!(n, 10);
        assert_eq!(
            *quota.used.lock().unwrap(),
            10,
            "only the accepted bytes should remain reserved"
        );

        // Failed write: the full reservation must be released.
        let quota = DiskQuota::new(100);
        let mut writer = QuotaWriter {
            inner: Box::new(ControlledWriter {
                outcome: Poll::Ready(Err(io::Error::other("boom"))),
            }),
            quota: quota.clone(),
        };
        writer.write(&[0u8; 40]).await.unwrap_err();
        assert_eq!(
            *quota.used.lock().unwrap(),
            0,
            "a failed write should release its entire reservation"
        );
    }
}
