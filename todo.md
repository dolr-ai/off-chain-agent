- remove ffmpeg@7 after done with offchain changes
- remove nix
- src/canister/delete/mod.rs — Canister Data Deletion & Reclamation
This module handles bulk deletion of user canister data — it's the cleanup/reclamation pipeline triggered via the QStash route /delete_and_reclaim_canisters. It's completely separate from snapshotting/backups.

Remove this