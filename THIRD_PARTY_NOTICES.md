# Third-party notices

`s7trace` includes a modified source copy of the `turbos7` crate under
`vendor/turbos7`.

- Package: `turbos7` 0.2.1
- Upstream author: ghouldevourer
- Published package: <https://crates.io/crates/turbos7/0.2.1>
- Declared license: MIT OR Apache-2.0
- Local modifications: see `vendor/turbos7/FORK-PATCH-NOTES.md`

The repository URL recorded in the published package metadata is no longer
available. This project therefore vendors the reviewed source snapshot instead
of depending on that repository.

All other Rust dependencies are resolved by Cargo. Their authors and license
metadata are available from their package manifests and the corresponding
entries in `Cargo.lock`.

Development scripts may optionally drive a separately obtained Snap7 server as
a local PLC simulator. Snap7 source code and binaries are not included in this
repository or in the `s7trace` distribution. Users who choose to obtain Snap7
do so separately and are responsible for its applicable license terms.

Siemens, SIMATIC, S7, and STEP 7 are trademarks or registered trademarks of
their respective owners. Their use here identifies compatible systems and does
not imply affiliation or endorsement.
