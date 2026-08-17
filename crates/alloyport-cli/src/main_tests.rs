//! Operator CLI behaviour: argument parsing, project intake bounds, and event rendering.

use super::*;

#[test]
fn first_product_fixture_passes_filesystem_inspection() -> Result<(), String> {
    let root =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/migrations/cuda-reduction-v1");
    let spec_path = root.join("migration-spec-v1.json");
    let spec_bytes = read_bounded_regular_file(&spec_path, MAX_SPEC_BYTES, "MigrationSpec")?;
    let spec: MigrationSpec = serde_json::from_slice(&spec_bytes)
        .map_err(|error| format!("invalid fixture spec: {error}"))?;
    let root = fs::canonicalize(root).map_err(|error| error.to_string())?;
    let files = load_declared_sources(&spec, &root)?;
    let report = inspect_migration_source(&spec, &files);

    assert!(report.passed, "{:?}", report.failures);
    assert_eq!(report.inspected_files, 5);
    Ok(())
}

#[test]
fn first_product_fixture_is_packaged_in_stable_path_order() -> Result<(), String> {
    let root =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/migrations/cuda-reduction-v1");
    let project = load_project(&root)?;

    assert_eq!(project.name, "cuda-reduction-v1");
    assert!(project.files.iter().any(|file| {
        Path::new(&file.path)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("cu"))
    }));
    assert!(
        project
            .files
            .windows(2)
            .all(|pair| pair[0].path < pair[1].path)
    );
    Ok(())
}
