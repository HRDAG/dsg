● Implementation Plan: Separating Concerns in DSG CLI

  Overview

  Refactor DSG CLI to cleanly separate data operations from display logic, making
  the codebase more testable, maintainable, and extensible. This plan introduces
  three new modules and refactors existing code to follow clear architectural
  boundaries.

  Architecture Layers

  1. Data Operations Layer: Pure business logic (no display)
  2. Display Layer: Pure presentation (no data fetching)
  3. CLI Layer: Thin orchestration between data and display

  New Modules to Create

  1. src/dsg/repository_discovery.py

  For pre-repository commands (init, list-repos):

  # Author: PB & Claude
  # Maintainer: PB
  # Original date: 2025.05.30
  # License: (c) HRDAG, 2025, GPL-2 or newer
  #
  # ------
  # src/dsg/repository_discovery.py

  import subprocess
  from pathlib import Path
  from typing import List, Optional
  from dataclasses import dataclass, field
  from datetime import datetime

  import orjson

  from dsg.host_utils import is_local_host


  @dataclass(frozen=True)
  class RepositoryInfo:
      """Information about a discovered repository."""
      name: str
      snapshot_id: Optional[str] = None
      timestamp: Optional[datetime] = None
      user: Optional[str] = None
      message: Optional[str] = None
      status: str = "active"  # active, error, uninitialized
      error_message: Optional[str] = None


  class RepositoryDiscovery:
      """Service for discovering repositories across different transports."""

      def list_repositories(self, host: str, project_path: Path) ->
  List[RepositoryInfo]:
          """List all repositories at the given host and path.

          Args:
              host: Hostname (localhost or remote)
              project_path: Base path containing repositories

          Returns:
              List of RepositoryInfo objects
          """
          if is_local_host(host):
              return self._list_local_repositories(project_path)
          else:
              return self._list_remote_repositories(host, project_path)

      def _list_local_repositories(self, project_path: Path) ->
  List[RepositoryInfo]:
          """List repositories on local filesystem."""
          repos = []

          if not project_path.exists() or not project_path.is_dir():
              return repos

          for item in project_path.iterdir():
              if not item.is_dir():
                  continue

              dsg_dir = item / ".dsg"
              if dsg_dir.exists() and dsg_dir.is_dir():
                  repo_info = self._read_repository_metadata(item.name, dsg_dir)
                  repos.append(repo_info)

          return repos

      def _list_remote_repositories(self, host: str, project_path: Path) ->
  List[RepositoryInfo]:
          """List repositories on remote host via SSH."""
          repos = []

          try:
              # Find .dsg directories
              ssh_cmd = [
                  "ssh", host,
                  f"find {project_path} -maxdepth 2 -name .dsg -type d 2>/dev/null"
              ]

              result = subprocess.run(ssh_cmd, capture_output=True, text=True,
  timeout=30)

              if result.returncode != 0:
                  return repos

              for line in result.stdout.strip().split('\n'):
                  if not line:
                      continue

                  dsg_path = Path(line)
                  repo_name = dsg_path.parent.name
                  repo_info = self._read_remote_repository_metadata(host, repo_name,
   dsg_path)
                  repos.append(repo_info)

          except (subprocess.TimeoutExpired, Exception):
              pass

          return repos

      def _read_repository_metadata(self, name: str, dsg_dir: Path) ->
  RepositoryInfo:
          """Read repository metadata from local .dsg directory."""
          try:
              # Try last-sync.json first, then manifest.json
              for filename in ["last-sync.json", "manifest.json"]:
                  manifest_file = dsg_dir / filename
                  if manifest_file.exists():
                      data = orjson.loads(manifest_file.read_bytes())
                      metadata = data.get("metadata", {})

                      return RepositoryInfo(
                          name=name,
                          snapshot_id=metadata.get("snapshot_id"),

  timestamp=self._parse_timestamp(metadata.get("created_at")),
                          user=metadata.get("created_by"),
                          message=metadata.get("snapshot_message", "Working
  directory" if filename == "manifest.json" else None),
                          status="active"
                      )

              # No manifest files found
              return RepositoryInfo(name=name, status="uninitialized")

          except Exception as e:
              return RepositoryInfo(
                  name=name,
                  status="error",
                  error_message=str(e)
              )

      def _read_remote_repository_metadata(self, host: str, name: str, dsg_path:
  Path) -> RepositoryInfo:
          """Read repository metadata from remote host."""
          try:
              # Try to read manifest files via SSH
              read_cmd = [
                  "ssh", host,
                  f"cat {dsg_path}/last-sync.json 2>/dev/null || cat
  {dsg_path}/manifest.json 2>/dev/null || echo '{{}}'"
              ]

              result = subprocess.run(read_cmd, capture_output=True, text=True,
  timeout=10)

              if result.returncode == 0 and result.stdout.strip():
                  data = orjson.loads(result.stdout.strip())
                  metadata = data.get("metadata", {})

                  if metadata:
                      return RepositoryInfo(
                          name=name,
                          snapshot_id=metadata.get("snapshot_id"),

  timestamp=self._parse_timestamp(metadata.get("created_at")),
                          user=metadata.get("created_by"),
                          message=metadata.get("snapshot_message"),
                          status="active"
                      )

              return RepositoryInfo(name=name, status="uninitialized")

          except Exception as e:
              return RepositoryInfo(
                  name=name,
                  status="error",
                  error_message=str(e)
              )

      def _parse_timestamp(self, timestamp_str: Optional[str]) ->
  Optional[datetime]:
          """Parse ISO timestamp string to datetime."""
          if not timestamp_str:
              return None
          try:
              return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
          except (ValueError, AttributeError):
              return None

  2. src/dsg/local_operations.py

  For local-only commands:

  # Author: PB & Claude
  # Maintainer: PB
  # Original date: 2025.05.30
  # License: (c) HRDAG, 2025, GPL-2 or newer
  #
  # ------
  # src/dsg/local_operations.py

  import gzip
  from pathlib import Path
  from typing import List, Optional, Dict, Any
  from dataclasses import dataclass
  from datetime import datetime

  import orjson

  from dsg.manifest import Manifest


  @dataclass(frozen=True)
  class SnapshotInfo:
      """Information about a snapshot from local archives."""
      snapshot_id: str
      timestamp: datetime
      user: str
      message: str
      file_count: int
      previous_snapshot: Optional[str] = None


  @dataclass(frozen=True)
  class FileHistory:
      """History entry for a file."""
      timestamp: datetime
      user: str
      message: str
      hash: str
      snapshot_id: str


  class LocalRepository:
      """Operations on local .dsg metadata."""

      def __init__(self, project_root: Path):
          self.root = project_root
          self.dsg_dir = project_root / ".dsg"
          self.archive_dir = self.dsg_dir / "archive"

      def get_last_sync_manifest(self) -> Optional[Manifest]:
          """Load the last sync manifest."""
          last_sync = self.dsg_dir / "last-sync.json"
          if not last_sync.exists():
              return None

          data = orjson.loads(last_sync.read_bytes())
          return Manifest.from_dict(data)

      def get_current_manifest(self) -> Optional[Manifest]:
          """Load the current working manifest."""
          manifest_file = self.dsg_dir / "manifest.json"
          if not manifest_file.exists():
              return None

          data = orjson.loads(manifest_file.read_bytes())
          return Manifest.from_dict(data)

      def get_snapshot_history(
          self,
          limit: Optional[int] = None,
          since: Optional[datetime] = None,
          author: Optional[str] = None
      ) -> List[SnapshotInfo]:
          """Get snapshot history from archives.

          Args:
              limit: Maximum number of snapshots to return
              since: Only return snapshots after this date
              author: Filter by author/user

          Returns:
              List of SnapshotInfo objects, newest first
          """
          snapshots = []

          if not self.archive_dir.exists():
              return snapshots

          # Get all archive files, sorted newest first
          archive_files = sorted(
              self.archive_dir.glob("*.json.gz"),
              key=lambda p: p.stat().st_mtime,
              reverse=True
          )

          for archive_file in archive_files:
              if limit and len(snapshots) >= limit:
                  break

              try:
                  with gzip.open(archive_file, 'rb') as f:
                      data = orjson.loads(f.read())
                      metadata = data.get("metadata", {})

                      # Parse timestamp
                      timestamp = self._parse_timestamp(metadata.get("created_at"))
                      if not timestamp:
                          continue

                      # Apply filters
                      if since and timestamp < since:
                          continue
                      if author and metadata.get("created_by") != author:
                          continue

                      # Count files
                      file_count = len(data.get("entries", []))

                      snapshot = SnapshotInfo(
                          snapshot_id=metadata.get("snapshot_id", "unknown"),
                          timestamp=timestamp,
                          user=metadata.get("created_by", "unknown"),
                          message=metadata.get("snapshot_message", ""),
                          file_count=file_count,
                          previous_snapshot=metadata.get("previous_snapshot")
                      )

                      snapshots.append(snapshot)

              except Exception:
                  # Skip corrupted archives
                  continue

          return snapshots

      def get_file_history(self, file_path: str) -> List[FileHistory]:
          """Get modification history for a specific file.

          Args:
              file_path: Path to file relative to project root

          Returns:
              List of FileHistory objects, newest first
          """
          history = []

          # First check current manifest
          current = self.get_last_sync_manifest()
          if current and file_path in current.entries:
              entry = current.entries[file_path]
              history.append(FileHistory(
                  timestamp=self._parse_timestamp(current.metadata.created_at) or
  datetime.now(),
                  user=current.metadata.created_by,
                  message=current.metadata.snapshot_message or "Current",
                  hash=entry.hash[:8],  # First 8 chars
                  snapshot_id=current.metadata.snapshot_id or "current"
              ))

          # Then check archives
          if self.archive_dir.exists():
              archive_files = sorted(
                  self.archive_dir.glob("*.json.gz"),
                  key=lambda p: p.stat().st_mtime,
                  reverse=True
              )

              last_hash = None
              for archive_file in archive_files:
                  try:
                      with gzip.open(archive_file, 'rb') as f:
                          data = orjson.loads(f.read())
                          entries = data.get("entries", {})

                          if file_path in entries:
                              entry = entries[file_path]
                              current_hash = entry.get("hash", "")[:8]

                              # Only add if hash changed
                              if current_hash != last_hash:
                                  metadata = data.get("metadata", {})
                                  timestamp =
  self._parse_timestamp(metadata.get("created_at"))

                                  if timestamp:
                                      history.append(FileHistory(
                                          timestamp=timestamp,
                                          user=metadata.get("created_by",
  "unknown"),
                                          message=metadata.get("snapshot_message",
  ""),
                                          hash=current_hash,
                                          snapshot_id=metadata.get("snapshot_id",
  "unknown")
                                      ))

                                  last_hash = current_hash

                  except Exception:
                      continue

          return history

      def _parse_timestamp(self, timestamp_str: Optional[str]) ->
  Optional[datetime]:
          """Parse ISO timestamp string to datetime."""
          if not timestamp_str:
              return None
          try:
              return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
          except (ValueError, AttributeError):
              return None

  3. Update src/dsg/display.py

  Add display functions for new data types:

  # Add to src/dsg/display.py

  from typing import List
  from rich.table import Table
  from rich.console import Console

  from dsg.repository_discovery import RepositoryInfo
  from dsg.local_operations import SnapshotInfo, FileHistory


  def repositories_to_table(
      repos: List[RepositoryInfo],
      host: str,
      project_path: Path,
      verbose: bool = False
  ) -> Table:
      """Convert repository list to rich table.

      Args:
          repos: List of RepositoryInfo objects
          host: Host where repositories are located
          project_path: Base path for repositories
          verbose: Show additional details

      Returns:
          Rich Table object
      """
      table = Table(title=f"DSG Repositories at {host}:{project_path}")
      table.add_column("Repository", style="cyan", no_wrap=True)
      table.add_column("Last Snapshot", style="yellow", no_wrap=True)
      table.add_column("Timestamp", style="green", no_wrap=True)
      table.add_column("User", style="blue", no_wrap=True)

      if verbose:
          table.add_column("Message", style="white")
          table.add_column("Status", style="white")

      for repo in repos:
          # Format timestamp
          timestamp_str = "Unknown"
          if repo.timestamp:
              timestamp_str = repo.timestamp.strftime("%Y-%m-%d %H:%M")

          # Color-code snapshot status
          snapshot_id = repo.snapshot_id or "None"
          if snapshot_id.startswith("s") and snapshot_id[1:].isdigit():
              snapshot_style = f"[green]{snapshot_id}[/green]"
          elif snapshot_id == "Working":
              snapshot_style = f"[yellow]{snapshot_id}[/yellow]"
          elif snapshot_id in ("None", "Error"):
              snapshot_style = f"[red]{snapshot_id}[/red]"
          else:
              snapshot_style = snapshot_id

          row = [
              repo.name,
              snapshot_style,
              timestamp_str,
              repo.user or "Unknown"
          ]

          if verbose:
              message = repo.message or repo.error_message or ""
              if len(message) > 50:
                  message = message[:47] + "..."
              row.extend([message, repo.status])

          table.add_row(*row)

      return table


  def format_repository_count(repos: List[RepositoryInfo]) -> str:
      """Format repository count summary."""
      total = len(repos)
      active = sum(1 for r in repos if r.status == "active")
      errors = sum(1 for r in repos if r.status == "error")
      uninitialized = sum(1 for r in repos if r.status == "uninitialized")

      parts = [f"Found {total} repositories"]
      if active:
          parts.append(f"{active} active")
      if uninitialized:
          parts.append(f"{uninitialized} uninitialized")
      if errors:
          parts.append(f"[red]{errors} with errors[/red]")

      return " - ".join(parts)


  def snapshot_history_to_table(
      snapshots: List[SnapshotInfo],
      verbose: bool = False
  ) -> Table:
      """Convert snapshot history to rich table."""
      table = Table(title="Snapshot History")
      table.add_column("Snapshot", style="yellow", no_wrap=True)
      table.add_column("Date", style="green", no_wrap=True)
      table.add_column("User", style="blue", no_wrap=True)
      table.add_column("Files", style="cyan", no_wrap=True, justify="right")
      table.add_column("Message", style="white")

      for snapshot in snapshots:
          date_str = snapshot.timestamp.strftime("%Y-%m-%d %H:%M")
          message = snapshot.message
          if not verbose and len(message) > 50:
              message = message[:47] + "..."

          table.add_row(
              snapshot.snapshot_id,
              date_str,
              snapshot.user,
              str(snapshot.file_count),
              message
          )

      return table


  def file_history_to_table(
      history: List[FileHistory],
      file_path: str
  ) -> Table:
      """Convert file history to rich table."""
      table = Table(title=f"History for {file_path}")
      table.add_column("User", style="blue", no_wrap=True)
      table.add_column("Date", style="green", no_wrap=True)
      table.add_column("Snapshot", style="yellow", no_wrap=True)
      table.add_column("Hash", style="cyan", no_wrap=True)
      table.add_column("Message", style="white")

      for entry in history:
          date_str = entry.timestamp.strftime("%Y-%m-%d %H:%M")
          message = entry.message
          if len(message) > 40:
              message = message[:37] + "..."

          table.add_row(
              entry.user,
              date_str,
              entry.snapshot_id,
              entry.hash,
              message
          )

      return table

  Updated CLI Commands

  Example: Refactored list-repos command

  # In src/dsg/cli.py

  @app.command(name="list-repos")
  def list_repos(
      verbose: bool = typer.Option(False, "--verbose", "-v", help="Show additional
  details")
  ):
      """List all available DSG repositories."""
      try:
          # 1. Load config (existing function)
          config = load_repository_discovery_config()

          # Validate required fields
          if not config.default_host or not config.default_project_path:
              console.print("[red]Error: Missing required config fields[/red]")
              raise typer.Exit(1)

          # 2. Get data (pure operation, no display)
          discovery = RepositoryDiscovery()
          repos = discovery.list_repositories(
              config.default_host,
              Path(config.default_project_path)
          )

          # 3. Display data (pure presentation, no data fetching)
          if not repos:
              console.print(f"No DSG repositories found at
  {config.default_host}:{config.default_project_path}")
              return

          table = repositories_to_table(
              repos,
              config.default_host,
              Path(config.default_project_path),
              verbose=verbose
          )
          console.print(table)
          console.print(format_repository_count(repos))

      except FileNotFoundError as e:
          console.print(f"[red]Config error: {e}[/red]")
          raise typer.Exit(1)
      except Exception as e:
          console.print(f"[red]Error: {e}[/red]")
          raise typer.Exit(1)

  Example: New log command

  @app.command()
  def log(
      repo: Optional[str] = typer.Option(None, "--repo", help="Repository name"),
      limit: Optional[int] = typer.Option(None, "--limit", "-n", help="Limit number
  of snapshots"),
      since: Optional[str] = typer.Option(None, "--since", help="Show snapshots
  since date (YYYY-MM-DD)"),
      author: Optional[str] = typer.Option(None, "--author", help="Filter by
  author/user"),
      verbose: bool = typer.Option(False, "--verbose", "-v", help="Show full
  messages")
  ):
      """Show snapshot history for the repository."""
      try:
          # 1. Parse arguments
          since_date = None
          if since:
              try:
                  since_date = datetime.fromisoformat(since)
              except ValueError:
                  console.print("[red]Error: Invalid date format. Use
  YYYY-MM-DD[/red]")
                  raise typer.Exit(1)

          # 2. Get data (pure operation)
          local_repo = LocalRepository(Path.cwd())
          snapshots = local_repo.get_snapshot_history(
              limit=limit,
              since=since_date,
              author=author
          )

          # 3. Display data (pure presentation)
          if not snapshots:
              console.print("No snapshots found matching criteria")
              return

          table = snapshot_history_to_table(snapshots, verbose=verbose)
          console.print(table)

      except Exception as e:
          console.print(f"[red]Error: {e}[/red]")
          raise typer.Exit(1)

  Example: New blame command

  @app.command()
  def blame(
      file: str = typer.Argument(..., help="File path to show modification
  history"),
      repo: Optional[str] = typer.Option(None, "--repo", help="Repository name")
  ):
      """Show modification history for a file."""
      try:
          # 1. Validate file exists
          file_path = Path(file)
          if not file_path.exists():
              console.print(f"[red]Error: File '{file}' not found[/red]")
              raise typer.Exit(1)

          # 2. Get data (pure operation)
          local_repo = LocalRepository(Path.cwd())
          history = local_repo.get_file_history(str(file_path))

          # 3. Display data (pure presentation)
          if not history:
              console.print(f"No history found for {file}")
              return

          table = file_history_to_table(history, str(file_path))
          console.print(table)

      except Exception as e:
          console.print(f"[red]Error: {e}[/red]")
          raise typer.Exit(1)

  Implementation Steps

  1. Create new modules:
    - src/dsg/repository_discovery.py
    - src/dsg/local_operations.py
    - Update src/dsg/display.py with new display functions
  2. Refactor existing commands:
    - Move _list_local_repositories and _list_remote_repositories from cli.py to
  RepositoryDiscovery
    - Update list-repos command to use new architecture
    - Remove display logic from data operations
  3. Implement local-only commands:
    - log - using LocalRepository.get_snapshot_history()
    - blame - using LocalRepository.get_file_history()
    - Update status (without --remote) to use LocalRepository
  4. Add comprehensive tests:
    - Unit tests for RepositoryDiscovery (mock SSH calls)
    - Unit tests for LocalRepository (use temp directories)
    - Integration tests for display functions
  5. Future backend-based commands will use existing pattern:
  backend = create_backend(config)
  # Use backend for remote operations

  Benefits

  1. Clean separation: Each layer has a single responsibility
  2. Testability: Can test data operations without display logic
  3. Reusability: Other tools can import and use these operations
  4. Type safety: Clear data contracts with dataclasses
  5. Extensibility: Easy to add new transport methods or display formats

  This architecture will make the codebase much easier to work with as we implement
  the remaining commands.
