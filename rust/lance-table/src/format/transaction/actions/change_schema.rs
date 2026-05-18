// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! `ChangeSchema` — overwrite the manifest schema.

use super::{
    Action, ActionOutput, ManifestMask, ManifestPath, TransactionError, TxnContext, WorkingState,
};

/// Models Overwrite's schema-replacement step; no field-ID continuity check
/// (the writer is asserting a clean reset).
#[derive(Debug, Clone)]
pub struct ChangeSchema {
    pub new_schema: lance_core::datatypes::Schema,
}

impl Action for ChangeSchema {
    fn reads(&self) -> ManifestMask {
        ManifestMask::empty()
    }

    fn writes(&self) -> ManifestMask {
        ManifestMask::singleton(ManifestPath::Schema)
    }

    fn validate(&self, _state: &WorkingState) -> Result<(), TransactionError> {
        Ok(())
    }

    fn rebase(
        &mut self,
        _current: &WorkingState,
        _concurrent: &[Box<dyn Action>],
    ) -> Result<(), TransactionError> {
        Ok(())
    }

    fn apply(
        &self,
        state: &mut WorkingState,
        _ctx: &mut TxnContext,
    ) -> Result<ActionOutput, TransactionError> {
        state.manifest.schema = self.new_schema.clone();
        Ok(ActionOutput::None)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
