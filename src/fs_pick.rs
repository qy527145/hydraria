//! Native OS file/directory pickers, exposed via `POST /api/fs/pick`.
//!
//! The frontend can't read absolute paths from `<input type="file">` for
//! security reasons, so we shell out to the OS-native dialog instead:
//!
//! * macOS — `osascript` driving AppleScript's `choose file` / `choose folder`
//!   / `choose file name`. Dialog appears in front of the user's running
//!   Hydraria process — works for both `hydraria` launched from a terminal
//!   and from a `.app` bundle.
//!
//! * Windows — PowerShell driving `System.Windows.Forms.OpenFileDialog` /
//!   `FolderBrowserDialog` / `SaveFileDialog`. Works without extra deps; the
//!   ShowDialog() call is modal and returns the picked path.
//!
//! * Other platforms (Linux, BSDs, …) — currently unsupported; the endpoint
//!   returns a 501-ish error so the frontend falls back to its plain text
//!   input. Adding zenity / kdialog support later is a localized change.
//!
//! The native dialogs block the OS GUI thread; we run them in
//! `spawn_blocking` so the tokio runtime stays responsive.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::process::Command;

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PickKind {
    /// Open existing file.
    OpenFile,
    /// Open / pick a directory.
    OpenDirectory,
    /// Save-as dialog (returns a destination path; file does not need to exist).
    SaveFile,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PickRequest {
    pub kind: PickKind,
    /// Suggested initial filename when `kind = SaveFile`. Ignored otherwise.
    #[serde(default)]
    pub default_name: Option<String>,
    /// Dialog title shown to the user. Ignored on platforms whose native
    /// dialogs don't accept one.
    #[serde(default)]
    pub title: Option<String>,
}

impl Default for PickKind {
    fn default() -> Self {
        PickKind::OpenFile
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PickResponse {
    /// `None` when the user cancelled the dialog.
    pub path: Option<String>,
}

/// Block-and-pick. The caller (route handler) wraps this in spawn_blocking;
/// the function itself shells out to a child process and blocks waiting for
/// it. Returns `Ok(None)` for user-cancellations and `Err` for missing-
/// binary / permission / unsupported-platform errors.
pub fn pick(req: PickRequest) -> Result<PickResponse, String> {
    let path = pick_inner(req)?;
    Ok(PickResponse {
        path: path.map(|p| p.to_string_lossy().into_owned()),
    })
}

#[cfg(target_os = "macos")]
fn pick_inner(req: PickRequest) -> Result<Option<PathBuf>, String> {
    // Build an AppleScript expression that returns a POSIX path. Each
    // dialog flavor is one line — keeps quoting simple.
    let title = req
        .title
        .as_deref()
        .map(escape_applescript)
        .unwrap_or_else(|| "选择文件".to_string());
    let default_name = req
        .default_name
        .as_deref()
        .map(escape_applescript)
        .unwrap_or_default();

    let script = match req.kind {
        PickKind::OpenFile => format!(
            r#"POSIX path of (choose file with prompt "{}")"#,
            title
        ),
        PickKind::OpenDirectory => format!(
            r#"POSIX path of (choose folder with prompt "{}")"#,
            title
        ),
        PickKind::SaveFile => {
            if default_name.is_empty() {
                format!(
                    r#"POSIX path of (choose file name with prompt "{}")"#,
                    title
                )
            } else {
                format!(
                    r#"POSIX path of (choose file name with prompt "{}" default name "{}")"#,
                    title, default_name
                )
            }
        }
    };

    let out = Command::new("osascript")
        .arg("-e")
        .arg(&script)
        .output()
        .map_err(|e| format!("osascript spawn: {}", e))?;
    if !out.status.success() {
        // AppleScript exits with status 1 when the user cancels via Cmd-.
        // Distinguish "cancel" (stderr contains "-128") from real errors so
        // the UI can quietly do nothing for the former.
        let err = String::from_utf8_lossy(&out.stderr);
        if err.contains("-128") || err.trim().is_empty() {
            return Ok(None);
        }
        return Err(format!("osascript: {}", err.trim()));
    }
    let path = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if path.is_empty() {
        return Ok(None);
    }
    Ok(Some(PathBuf::from(path)))
}

#[cfg(target_os = "macos")]
fn escape_applescript(s: &str) -> String {
    // AppleScript string literal: escape backslash and double-quote.
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(target_os = "windows")]
fn pick_inner(req: PickRequest) -> Result<Option<PathBuf>, String> {
    // PowerShell snippet selected per kind. We use `WriteLine` to stdout so
    // the parent process can capture it; on cancel we emit an empty line.
    // STA is required for Windows Forms dialogs.
    let title = req.title.as_deref().unwrap_or("Choose a file");
    let default_name = req.default_name.as_deref().unwrap_or("");
    let escape = |s: &str| s.replace('`', "``").replace('"', "`\"");
    let title_e = escape(title);
    let default_e = escape(default_name);

    let script = match req.kind {
        PickKind::OpenFile => format!(
            r#"Add-Type -AssemblyName System.Windows.Forms | Out-Null
$f = New-Object System.Windows.Forms.OpenFileDialog
$f.Title = "{title}"
if ($f.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK) {{ Write-Output $f.FileName }}"#,
            title = title_e
        ),
        PickKind::OpenDirectory => format!(
            r#"Add-Type -AssemblyName System.Windows.Forms | Out-Null
$f = New-Object System.Windows.Forms.FolderBrowserDialog
$f.Description = "{title}"
if ($f.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK) {{ Write-Output $f.SelectedPath }}"#,
            title = title_e
        ),
        PickKind::SaveFile => format!(
            r#"Add-Type -AssemblyName System.Windows.Forms | Out-Null
$f = New-Object System.Windows.Forms.SaveFileDialog
$f.Title = "{title}"
$f.FileName = "{default_name}"
if ($f.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK) {{ Write-Output $f.FileName }}"#,
            title = title_e,
            default_name = default_e
        ),
    };

    let out = Command::new("powershell")
        .arg("-NoProfile")
        .arg("-NonInteractive")
        .arg("-STA")
        .arg("-Command")
        .arg(&script)
        .output()
        .map_err(|e| format!("powershell spawn: {}", e))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr).trim().to_string();
        if err.is_empty() {
            return Ok(None);
        }
        return Err(format!("powershell: {}", err));
    }
    let path = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if path.is_empty() {
        return Ok(None);
    }
    Ok(Some(PathBuf::from(path)))
}

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
fn pick_inner(_req: PickRequest) -> Result<Option<PathBuf>, String> {
    Err("native file picker not supported on this platform; please paste the path manually".into())
}

/// True when this build can pop a native file picker. Used by the UI so it
/// only renders the "Browse..." button on supported platforms.
pub fn is_supported() -> bool {
    cfg!(any(target_os = "macos", target_os = "windows"))
}
