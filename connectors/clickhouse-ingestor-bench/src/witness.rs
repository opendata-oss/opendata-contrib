//! JSONL witness writer used by each `ack_invariant_checks`
//! scenario. Every observed event is one line of JSON; the bench
//! report stamps the file path in
//! `correctness.json.ack_invariant_checks[*].evidence`.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;

/// Append-mode JSONL writer. Flushes on `close()`. One witness
/// file per scenario; the [`crate::output::CorrectnessReport`] points
/// `ack_invariant_checks[*].evidence` at the relative path under
/// `raw/correctness/`.
pub struct WitnessWriter {
    path: PathBuf,
    inner: BufWriter<File>,
}

impl WitnessWriter {
    pub fn create(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = File::create(&path)?;
        Ok(Self {
            path,
            inner: BufWriter::new(file),
        })
    }

    pub fn write_event<E: Serialize>(&mut self, event: &E) -> std::io::Result<()> {
        let line = serde_json::to_string(event)?;
        self.inner.write_all(line.as_bytes())?;
        self.inner.write_all(b"\n")?;
        Ok(())
    }

    pub fn close(mut self) -> std::io::Result<()> {
        self.inner.flush()?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}
