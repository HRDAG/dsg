# DSG Checkout Command Proposal

## Overview

We're proposing a new `checkout` command for DSG that enables users to create local copies of remote repositories when they don't have an existing `.dsg/` directory. This command fills the gap between repository initialization and ongoing synchronization.

## Command Purpose

The `checkout` command handles two scenarios:
1. **Repository checkout**: Get the latest version from an existing remote repository
2. **Repository initialization**: Create a new repository on both local and remote when neither exists

## URI-Based Repository Specification

Instead of multiple command-line arguments, we've designed a URI format that's:
- Easy to share between collaborators
- Familiar to users of git, rsync, and other tools  
- Extensible to different transport and storage backends

### URI Format
```
dsg+[transport]://[user@]host/repo_path/repo_name#type=repo_type
```

### Supported Transports
- **SSH**: Remote access via SSH (becomes local if hostname matches current machine)
- **rclone**: Cloud storage via rclone configurations
- **IPFS**: Distributed storage via IPFS (encrypted)

## Examples

```bash
# SSH to remote ZFS repository
dsg checkout dsg+ssh://scott/var/repos/zsd/BB#type=zfs

# SSH with specific username
dsg checkout dsg+ssh://alice@dataserver/projects/BB#type=xfs

# Cloud storage via rclone
dsg checkout dsg+rclone://gdrive/projects/BB

# Distributed IPFS repository
dsg checkout dsg+ipfs://did:key:z6Mkhn3rpi3pxisaGDX9jABfdWoyH5cKENd2Pgv9q8fRwqxC/BB --passphrase mypassword
```

## Key Design Decisions

### 1. URI Format Selection
We chose the service-like format (`dsg+ssh://`) over alternatives because:
- Standard URI parsing with Python's `urllib.parse`
- Clear separation of transport vs. storage backend
- Familiar to users from git remotes and web URLs
- Extensible for future transport types

### 2. Automatic Local Detection
SSH URIs automatically become local connections when the hostname matches the current machine. This eliminates the need for a separate `dsg+local://` format.

### 3. Transport-Specific Type Handling
- SSH repositories require `#type=zfs` or `#type=xfs` 
- rclone repositories determine type from rclone configuration
- IPFS repositories are inherently IPFS-typed

### 4. IPFS Encryption
IPFS repositories require encryption since content is potentially accessible to anyone with the hash. The passphrase is:
- Provided via `--passphrase` during checkout
- Stored in local `.dsg/config.yml` for future sync operations
- Protects against external IPFS discovery (not local access threats)

## Configuration Coupling

**Important**: All users of a repository must have aligned configurations:

### SSH Repositories
- UserConfig (name, email)
- ProjectConfig (repo settings) 
- Remote hostname resolution
- Local `~/.ssh/config` entries

### rclone Repositories  
- UserConfig (name, email)
- ProjectConfig (repo settings)
- Matching rclone.conf remote names and settings

### IPFS Repositories
- UserConfig (name, email)
- Shared IPFS space/DID access
- Same passphrase across team

This coupling is intentional - it ensures team members can collaborate seamlessly once initial setup is complete.

## Implementation Architecture

### Separation of Concerns
1. **Transport Layer**: How we connect (SSH, rclone, IPFS)
2. **Storage Backend**: Where data lives (ZFS, XFS, cloud services)
3. **Frontend**: CLI interface and manifest management
4. **Sync Logic**: Deciding what operations to perform

### Repository Operations
1. **Validation**: Check if remote repository exists and is accessible
2. **Initialization**: Create repository structure on both ends when missing
3. **Checkout**: Download existing repository metadata and files
4. **Configuration**: Inherit remote config or create from parameters

## Benefits for Collaborators

1. **Easy Repository Sharing**: Send a URI instead of complex setup instructions
2. **Consistent Configuration**: Repository config is inherited from remote
3. **Flexible Storage**: Support for local, cloud, and distributed storage
4. **Future-Proof**: Extensible to new transport and storage types

## Next Steps

1. Implement URI parsing using `urllib.parse`
2. Extend backend system to support transport abstraction
3. Add IPFS encryption/decryption capabilities
4. Create comprehensive tests for all transport types
5. Update documentation with team setup procedures

## Questions for Collaborators

1. Does the URI format feel intuitive for your workflows?
2. Are there other transport types we should consider?
3. How do you envision setting up team configurations?
4. Any concerns about the IPFS encryption approach?

---

**Note**: This proposal extends the existing DSG architecture while maintaining compatibility with current `sync` workflows. After checkout, users continue using `dsg sync` for ongoing repository management.