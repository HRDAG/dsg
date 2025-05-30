# Config Models Redesign Plan

## Overview

Redesign config_manager.py to support the new transport-agnostic architecture with proper type safety and validation.

## Key Changes

1. **No backward compatibility** - clean slate redesign
2. **Transport-specific config models** instead of generic dicts
3. **Path types** for filesystem paths
4. **Proper validation** using Pydantic model validators

## New Config File Structure

### Project Config (`.dsgconfig.yml`)
```yaml
transport: ssh
ssh:
  host: scott
  path: /var/repos/zsd
  name: BB
  type: zfs

project:
  data_dirs:
    - input
    - output
    - frozen
  ignore:
    paths:
      - temp/
    names:
      - .DS_Store
    suffixes:
      - .tmp
```

### User Config (`~/.config/dsg/dsg.yml`)
```yaml
user_name: "Alice Smith"
user_id: "alice@example.com"

ssh:
  key_path: ~/.ssh/special_dsg_key

ipfs:
  passphrases:
    "did:key:z6Mkhn3rpi3pxisaGDX9jABfdWoyH5cKENd2Pgv9q8fRwqxC": "mypassword"
```

## New Model Definitions

### Transport-Specific Repository Configs

```python
class SSHRepositoryConfig(BaseModel):
    host: str
    path: Path  # Repository path on remote host
    name: str  
    type: Literal["zfs", "xfs"]

class RcloneRepositoryConfig(BaseModel):
    remote: str
    path: Path  # Path within the rclone remote
    name: str

class IPFSRepositoryConfig(BaseModel):
    did: str
    name: str
    encrypted: bool = True
```

### Project Config Model

```python
class ProjectSettings(BaseModel):
    data_dirs: set[str] = {"input", "output", "frozen"}
    ignore: IgnoreSettings

class IgnoreSettings(BaseModel):
    paths: set[str] = Field(default_factory=set)
    names: set[str] = Field(default_factory=lambda: {
        ".DS_Store", "__pycache__", ".Rdata", ".rdata", ".RData", ".Rproj.user"
    })
    suffixes: set[str] = Field(default_factory=lambda: {".tmp", ".pyc"})

class ProjectConfig(BaseModel):
    transport: Literal["ssh", "rclone", "ipfs"]
    
    # Transport-specific configs (only one will be set)
    ssh: Optional[SSHRepositoryConfig] = None
    rclone: Optional[RcloneRepositoryConfig] = None  
    ipfs: Optional[IPFSRepositoryConfig] = None
    
    # Project settings (stable across transports)
    project: ProjectSettings
    
    @model_validator(mode="after") 
    def validate_transport_config(self):
        # Ensure exactly one transport config is set
        configs = [self.ssh, self.rclone, self.ipfs]
        set_configs = [c for c in configs if c is not None]
        
        if len(set_configs) != 1:
            raise ValueError("Exactly one transport config must be set")
            
        # Check that the set config matches transport type
        if self.transport == "ssh" and self.ssh is None:
            raise ValueError("SSH config required when transport=ssh")
        elif self.transport == "rclone" and self.rclone is None:
            raise ValueError("rclone config required when transport=rclone")
        elif self.transport == "ipfs" and self.ipfs is None:
            raise ValueError("IPFS config required when transport=ipfs")
            
        return self
```

### User Config Models

```python
class SSHUserConfig(BaseModel):
    key_path: Optional[Path] = None  # Path to SSH private key

class RcloneUserConfig(BaseModel):
    config_path: Optional[Path] = None  # Path to rclone.conf

class IPFSUserConfig(BaseModel):
    passphrases: dict[str, str] = Field(default_factory=dict)  # DID -> passphrase

class UserConfig(BaseModel):
    user_name: str
    user_id: EmailStr
    
    # Optional security configs
    ssh: Optional[SSHUserConfig] = None
    rclone: Optional[RcloneUserConfig] = None
    ipfs: Optional[IPFSUserConfig] = None
```

## Config File Finders

```python
def find_user_config_path() -> Path:
    """Locate the user config file from common locations."""
    # Same as before - looks for ~/.config/dsg/dsg.yml

def find_project_config_path(start: Path | None = None) -> Path:
    """Walk up from start path looking for .dsgconfig.yml."""
    current = (start or Path.cwd()).resolve()
    for parent in [current] + list(current.parents):
        candidate = parent / ".dsgconfig.yml"
        if candidate.exists():
            return candidate
    
    raise FileNotFoundError("No .dsgconfig.yml found in this or any parent directory")
```

## Config Loading

```python
class Config(BaseModel):
    user: UserConfig
    project: ProjectConfig
    project_root: Path = Field(exclude=True)

    @classmethod
    def load(cls, start_path: Path | None = None) -> Config:
        # Load user config (required)
        user_config_path = find_user_config_path()
        user_config = UserConfig.load(user_config_path)
        
        # Load project config (required)
        project_config_path = find_project_config_path(start_path)
        project_root = project_config_path.parent
        
        with project_config_path.open("r", encoding="utf-8") as f:
            project_data = yaml.safe_load(f)
        project_config = ProjectConfig.model_validate(project_data)
        
        return cls(
            user=user_config,
            project=project_config,
            project_root=project_root
        )
```

## Benefits

1. **Type Safety**: Path validation, proper enums, structured data
2. **Transport Isolation**: Each transport has its own validated config
3. **Clear Validation**: Pydantic catches config errors early
4. **IDE Support**: Autocomplete and type checking
5. **Extensibility**: Easy to add new transport types
6. **No Ambiguity**: Clear structure vs. generic dicts

## Implementation Impact

- **config_manager.py**: Complete rewrite with new models
- **All tests**: Will need updates for new config structure
- **Backend creation**: Update to use new config models
- **CLI commands**: Update to work with new config loading