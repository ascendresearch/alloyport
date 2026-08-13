use super::*;
use std::collections::BTreeSet;

#[test]
fn candidate_episode_catalog_exposes_only_the_four_gated_tools() {
    let tools = candidate_episode_tool_definitions();
    assert!(tools.iter().all(|tool| tool.strict));
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            SUBMIT_CANDIDATE_BUNDLE_TOOL,
            REQUEST_SOURCE_GATE_TOOL,
            REQUEST_ASCEND_BUILD_TOOL,
            REQUEST_REDUCTION_CORRECTNESS_TOOL,
        ])
    );
    for tool in tools {
        assert_eq!(tool.input_schema["type"], "object");
        assert_eq!(tool.input_schema["additionalProperties"], false);
        assert!(tool.input_schema["required"].is_array());
    }
}
