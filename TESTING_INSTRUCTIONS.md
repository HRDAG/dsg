# SSH Backend Testing Instructions

Since Claude runs inside a container, real SSH testing needs to be done on the host system. Here's how to test the backend factory with real SSH scenarios.

## Setup on Scott (Remote Host)

First, set up a test repository on scott:

```bash
# On scott machine
mkdir -p /tmp/dsg-ssh-test
cd /tmp/dsg-ssh-test
mkdir -p .dsg input output

# Create test data
echo "test data 1" > input/data1.csv
echo "test data 2" > input/data2.csv
echo "result data" > output/result.csv

# Create .dsgconfig.yml
cat > .dsgconfig.yml << 'EOF'
transport: ssh
ssh:
  host: scott
  path: /tmp
  name: dsg-ssh-test
  type: zfs
project:
  data_dirs: [input, output]
EOF
```

## Testing on Host System

### Test 1: Backend Factory Detection

Run this on your host system (outside container):

```bash
cd /path/to/dsg
export UV_LINK_MODE=copy
uv run python test_manual_ssh.py
```

When prompted, enter scott's actual hostname. This tests:
- ✅ Remote SSH detection (should create SSHBackend)
- ✅ Localhost detection (current hostname should create LocalhostBackend)
- ✅ SSH accessibility checks

### Test 2: Factory Detection Logic

```bash
uv run python test_factory_detection.py
```

This validates the core detection algorithms.

## Expected Results

### Scenario A: True Remote SSH
- **Config**: `host=scott, path=/tmp, name=dsg-ssh-test`
- **Expected**: `SSHBackend` created
- **Test**: SSH connectivity and accessibility checks

### Scenario B: SSH to Current Hostname
- **Config**: `host=$(hostname), path=/tmp, name=test`
- **Expected**: `LocalhostBackend` created (optimization)
- **Test**: Factory correctly detects localhost via hostname

### Scenario D: Explicit Localhost
- **Config**: `host=localhost` or `host=127.0.0.1`
- **Expected**: `LocalhostBackend` created
- **Test**: Standard localhost detection

## Debugging SSH Issues

If SSH tests fail, check:

1. **SSH connectivity**:
   ```bash
   ssh scott "echo 'SSH working'"
   ```

2. **Repository exists**:
   ```bash
   ssh scott "ls -la /tmp/dsg-ssh-test/"
   ```

3. **Permissions**:
   ```bash
   ssh scott "test -r /tmp/dsg-ssh-test/.dsg && echo 'Readable' || echo 'Not readable'"
   ```

## Test Results to Report

Please run the tests and report:

1. **Backend types created** for each scenario
2. **SSH accessibility results** (OK/error messages)
3. **Any unexpected behavior** or errors

This will validate that our intelligent backend factory works correctly in real-world SSH scenarios.