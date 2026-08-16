use super::*;
use std::collections::BTreeSet;

#[test]
fn candidate_episode_catalog_is_closed_and_grants_authority_only_through_gates() {
    let tools = candidate_episode_tool_definitions();
    assert!(tools.iter().all(|tool| tool.strict));
    // The catalog is fixed and closed. Model arguments cannot choose a worker, an image, a device,
    // a command, the corpus, or the tolerance; that property is what this set is guarding, not the
    // count. `read_build_diagnostics` is an instrument: it returns evidence the pipeline already
    // published about this migration and approves nothing.
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            SUBMIT_CANDIDATE_BUNDLE_TOOL,
            REQUEST_SOURCE_GATE_TOOL,
            REQUEST_ASCEND_BUILD_TOOL,
            READ_BUILD_DIAGNOSTICS_TOOL,
            READ_REFERENCE_TOOL,
            REQUEST_REDUCTION_CORRECTNESS_TOOL,
        ])
    );
    for tool in tools {
        assert_eq!(tool.input_schema["type"], "object");
        assert_eq!(tool.input_schema["additionalProperties"], false);
        assert!(tool.input_schema["required"].is_array());
    }
}
