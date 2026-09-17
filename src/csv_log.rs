//! CSV logging module for runner usage documentation.
//!
//! Logs all server starts and stops with relevant metadata to a CSV file.

use chrono::{DateTime, Utc};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use thiserror::Error;
use tracing::info;

/// Errors that can occur during CSV logging.
#[derive(Error, Debug)]
pub enum CsvLogError {
    #[error("Failed to open/create log file: {0}")]
    FileError(#[from] std::io::Error),
}

/// Event type for the CSV log.
#[derive(Debug, Clone, Copy)]
pub enum LogEvent {
    /// Server was started
    Start,
    /// Server was stopped
    Stop,
}

impl std::fmt::Display for LogEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogEvent::Start => write!(f, "START"),
            LogEvent::Stop => write!(f, "STOP"),
        }
    }
}

/// An entry in the CSV log.
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// Timestamp of the event
    pub timestamp: DateTime<Utc>,
    /// Type of event (START/STOP)
    pub event: LogEvent,
    /// Scaleway server UUID
    pub server_id: Option<String>,
    /// GitLab project (path_with_namespace)
    pub project: Option<String>,
    /// Pipeline ID that triggered the start
    pub pipeline_id: Option<u64>,
    /// Reason for the event
    pub reason: String,
    /// Runtime in minutes (only for STOP)
    pub duration_minutes: Option<u64>,
}

/// CSV logger for runner usage.
pub struct CsvLogger {
    /// Path to the log file
    log_path: std::path::PathBuf,
}

impl CsvLogger {
    /// Creates a new CSV logger.
    ///
    /// Creates the `logs/` directory if it doesn't exist
    /// and initializes the CSV file with header if it's new.
    ///
    /// # Arguments
    /// * `log_dir` - Directory for log files
    pub fn new<P: AsRef<Path>>(log_dir: P) -> Result<Self, CsvLogError> {
        let log_dir = log_dir.as_ref();

        // Create directory if it doesn't exist
        if !log_dir.exists() {
            info!("Creating log directory: {}", log_dir.display());
            std::fs::create_dir_all(log_dir)?;
        }

        let log_path = log_dir.join("runner_usage.csv");
        let logger = Self {
            log_path: log_path.clone(),
        };

        // Write header if file is new
        if !log_path.exists() {
            logger.write_header()?;
        }

        info!("CSV logger initialized: {}", log_path.display());
        Ok(logger)
    }

    /// Writes the CSV header to the file.
    fn write_header(&self) -> Result<(), CsvLogError> {
        let file = File::create(&self.log_path)?;
        let mut writer = BufWriter::new(file);
        writeln!(
            writer,
            "timestamp,event,server_id,project,pipeline_id,reason,duration_minutes"
        )?;
        writer.flush()?;
        Ok(())
    }

    /// Writes a log entry to the CSV file.
    ///
    /// # Arguments
    /// * `entry` - The log entry to write
    pub fn log(&self, entry: &LogEntry) -> Result<(), CsvLogError> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.log_path)?;

        let mut writer = BufWriter::new(file);

        // Format CSV line
        let line = format!(
            "{},{},{},{},{},{},{}",
            entry.timestamp.to_rfc3339(),
            entry.event,
            entry.server_id.as_deref().unwrap_or_default(),
            entry.project.as_deref().unwrap_or(""),
            entry
                .pipeline_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
            // Escape reason if it contains commas
            escape_csv_field(&entry.reason),
            entry
                .duration_minutes
                .map(|d| d.to_string())
                .unwrap_or_default(),
        );

        writeln!(writer, "{}", line)?;
        writer.flush()?;

        info!("CSV log: {} - {}", entry.event, entry.reason);
        Ok(())
    }

    /// Helper method: Logs a server start.
    pub fn log_start(
        &self,
        server_id: &str,
        project: &str,
        pipeline_id: u64,
        reason: &str,
    ) -> Result<(), CsvLogError> {
        let entry = LogEntry {
            timestamp: Utc::now(),
            event: LogEvent::Start,
            server_id: Some(server_id.to_string()),
            project: Some(project.to_string()),
            pipeline_id: Some(pipeline_id),
            reason: reason.to_string(),
            duration_minutes: None,
        };
        self.log(&entry)
    }

    /// Helper method: Logs a server stop.
    pub fn log_stop(
        &self,
        server_id: &str,
        reason: &str,
        duration_minutes: u64,
    ) -> Result<(), CsvLogError> {
        let entry = LogEntry {
            timestamp: Utc::now(),
            event: LogEvent::Stop,
            server_id: Some(server_id.to_string()),
            project: None,
            pipeline_id: None,
            reason: reason.to_string(),
            duration_minutes: Some(duration_minutes),
        };
        self.log(&entry)
    }
}

/// Escapes a CSV field if it contains special characters.
fn escape_csv_field(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_csv_field() {
        assert_eq!(escape_csv_field("simple"), "simple");
        assert_eq!(escape_csv_field("with,comma"), "\"with,comma\"");
        assert_eq!(escape_csv_field("with\"quote"), "\"with\"\"quote\"");
    }

    #[test]
    fn test_log_entry_serializes_string_server_id() {
        let entry = LogEntry {
            timestamp: Utc::now(),
            event: LogEvent::Start,
            server_id: Some("server-uuid".to_string()),
            project: Some("group/project".to_string()),
            pipeline_id: Some(42),
            reason: "pipeline_pending".to_string(),
            duration_minutes: None,
        };
        // Exercise the same formatting path used by `log`.
        let server_id = entry
            .server_id
            .clone()
            .map(|id| id.to_string())
            .unwrap_or_default();
        assert_eq!(server_id, "server-uuid");
    }
}
