# Sync tool

## Relevant sync states

1. Client's current
2. Client's last
3. Remote's current
4. Remote's expected


## Sync components

- The manifest files for states 1-3
- The project files
- (any config? turn up/down verbosity?)


## Sync process

### outline

1. creating a new synced project (ie. client has it but not remote)
2. first sync of an existing project (ie. remote has it but not client)
3. kth sync of an existing project (ie. both client and remote have some version of it)

### rough details

#### creating a new synced project
- A .[tool-name] folder is created at the client's project's root.
- A .client-current manifest file is created inside the client's .[tool-name] folder.
- The project is initialized in the backend / sync location / etc.
- The .client-current manifest file is copied as .remote-current in the sync location.
- The .client-current manifest file is copied as .client-last inside the client's .[tool-name] folder
- (.client-current is deleted? or just ignored until next sync.)
    - My two cents: I think the client-current should really only exist at the moment the client initiates a sync. The tool shouldn't surveil a project for changed files and should rely on the user initiating a sync to check for changes. At that point, we may compare to .client-last to identify what the client thinks the changes are, and .remote-current to identify what the remote thinks the changes are, and both of those rely on an up-to-date .client-current. But, how to keep that truly current without babysitting the project or working from scratch.


#### first sync of an existing project
- Client receives the .remote-current manifest and copies of the project files according to that manifest
- .client-current and .client-last are copies of .remote-current
    - unless we create .client-current from scratch as a way of confirming the sync worked as expected?


#### kth sync of an existing project
- comparisons
    - .client-current vs. .client-last:
        - captures changes client expects to have made since last sync, or if both manifests agree, confirms no changes by client
    - .client-current vs. .remote-current:
        - captures actual changes made by client or potential disagreements (if .client-last and .remote-current also don't match) 
        - or if all 3 manifests agree, confirms no changes by this client or another, ie) nothing for sync to do
            - is it correct that there should never be a time when .client-current == .remote-current != .client-last? (assuming we make .client-current a copy of .client-last at the time of syncing)
    - .client-last vs. .remote-current:
        - captures changes made to the remote between client's last sync and client's current states, ie) potential conflict



_done._
