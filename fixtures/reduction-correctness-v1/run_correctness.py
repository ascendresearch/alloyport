#!/usr/bin/env python3
"""Trusted shell-free builder/runner for one controller-authored reduction bundle."""

from __future__ import annotations

import hashlib
import json
import pathlib
import re
import subprocess
import sys

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")

BUNDLE_ROOT = pathlib.Path("/alloyport/bundle")
WORK_ROOT = pathlib.Path("/alloyport/work")
BUNDLE_PATH = BUNDLE_ROOT / "execution-bundle.json"
CONFIG_PATH = BUNDLE_ROOT / "runner-config.json"
# The worker points TMPDIR inside this tmpfs, which starts empty; creating it keeps the toolchain
# from printing a fallback warning into the diagnostics a reader has to trust.
TEMPORARY_PATH = WORK_ROOT / "tmp"
HARNESS_PATH = WORK_ROOT / "reduction_harness.cpp"
BUILD_PATH = WORK_ROOT / "build"


def fail(detail: str) -> None:
    print(detail, file=sys.stderr)
    raise SystemExit(2)


def input_digest(case: dict[str, object]) -> str:
    kind_tags = {
        "valid": 1,
        "null_input": 2,
        "null_output": 3,
        "unsupported_size": 4,
    }
    payload = bytearray(b"alloyport-reduction-input-v1\0")
    payload.extend(str(case["case_id"]).encode("ascii"))
    payload.extend(int(case["repetition"]).to_bytes(2, "big"))
    payload.extend(int(case["elements"]).to_bytes(8, "big"))
    payload.extend(int(case["seed"]).to_bytes(4, "big"))
    payload.append(kind_tags[str(case["kind"])])
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def cpp_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=True)


def harness_source(bundle: dict[str, object], config: dict[str, object]) -> str:
    experiment = bundle["experiment"]
    # The symbol under test is carried by the controller-authored bundle. This runner is trusted and
    # ships with the worker, so a specimen name compiled into it would make onboarding a second
    # operator family an edit inside the trust boundary. The call SHAPE below —
    # int(const float *, size_t, float *) — is still fixed for the phase-1 scope.
    symbol = str(bundle["callable"]["public_symbol"])
    corpus = bundle["corpus"]
    role = str(bundle["role"])
    candidate_id = bundle["candidate_id"]
    cases = corpus["cases"]
    case_rows = []
    kind_names = {
        "valid": "Valid",
        "null_input": "NullInput",
        "null_output": "NullOutput",
        "unsupported_size": "UnsupportedSize",
    }
    for case in cases:
        case_rows.append(
            "    {"
            + ", ".join(
                [
                    cpp_string(str(case["case_id"])),
                    str(int(case["repetition"])),
                    str(int(case["elements"])) + "ULL",
                    str(int(case["seed"])) + "U",
                    "CaseKind::" + kind_names[str(case["kind"])],
                    cpp_string(input_digest(case)),
                ]
            )
            + "},"
        )
    candidate_json = "nullptr" if candidate_id is None else cpp_string(str(candidate_id))
    return f"""#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>

extern "C" int {symbol}(const float *, size_t, float *);

enum class CaseKind {{ Valid, NullInput, NullOutput, UnsupportedSize }};
struct Case {{
    const char *id;
    unsigned int repetition;
    unsigned long long elements;
    unsigned int seed;
    CaseKind kind;
    const char *input_digest;
}};

static uint32_t next_random(uint32_t *state) {{
    *state = *state * 1664525U + 1013904223U;
    return *state;
}}

static std::vector<float> make_input(size_t elements, uint32_t seed) {{
    std::vector<float> input(elements);
    for (float &value : input) {{
        const int32_t centered = static_cast<int32_t>(next_random(&seed) >> 8U) - (1 << 23);
        value = static_cast<float>(centered) / static_cast<float>(1 << 20);
    }}
    return input;
}}

// A second legitimate fp32 summation order of the same mathematics over the same bytes: a pairwise
// tree with a leaf block unrelated to the reference kernel's block size. It is not a check of the
// implementation and never decides a verdict; the oracle uses the distance between the two orders
// to measure how far a correct implementation may legitimately land from the authority. Without it
// the tolerance would have to be asserted, and an asserted tolerance either rejects correct ports
// or admits broken ones with nothing to say which.
static float pairwise_sum(const float *values, size_t count) {{
    if (count == 0) return 0.0F;
    if (count <= 37) {{
        float total = 0.0F;
        for (size_t index = 0; index < count; ++index) total += values[index];
        return total;
    }}
    const size_t half = count / 2;
    return pairwise_sum(values, half) + pairwise_sum(values + half, count - half);
}}

int main() {{
    static const Case cases[] = {{
{chr(10).join(case_rows)}
    }};
    const char *candidate_id = {candidate_json};
    std::printf("{{\\\"schema_version\\\":1,\\\"experiment_digest\\\":\\\"%s\\\","
                "\\\"role\\\":\\\"%s\\\",\\\"candidate_id\\\":",
                {cpp_string(str(experiment["experiment_digest"]))}, {cpp_string(role)});
    if (candidate_id == nullptr) std::printf("null");
    else std::printf("\\\"%s\\\"", candidate_id);
    std::printf(",\\\"implementation_digest\\\":\\\"%s\\\","
                "\\\"corpus_digest\\\":\\\"%s\\\","
                "\\\"environment_digest\\\":\\\"%s\\\","
                "\\\"implementation_invoked\\\":true,\\\"synchronized\\\":true,"
                "\\\"observations\\\":[",
                {cpp_string(str(bundle["implementation_digest"]))},
                {cpp_string(str(experiment["corpus_digest"]))},
                {cpp_string(str(config["environment_digest"]))});
    bool first = true;
    for (const Case &item : cases) {{
        std::vector<float> input = make_input(static_cast<size_t>(item.elements), item.seed);
        float output = -1.0F;
        const float *data = input.empty() ? nullptr : input.data();
        int status = 0;
        switch (item.kind) {{
            case CaseKind::Valid:
                status = {symbol}(data, input.size(), &output);
                break;
            case CaseKind::NullInput:
                status = {symbol}(nullptr, input.size(), &output);
                break;
            case CaseKind::NullOutput:
                status = {symbol}(data, input.size(), nullptr);
                break;
            case CaseKind::UnsupportedSize:
                status = {symbol}(data, input.size(), &output);
                break;
        }}
        uint32_t output_bits = 0;
        std::memcpy(&output_bits, &output, sizeof(output_bits));
        const float reordered = pairwise_sum(input.empty() ? nullptr : input.data(), input.size());
        uint32_t reorder_bits = 0;
        std::memcpy(&reorder_bits, &reordered, sizeof(reorder_bits));
        if (!first) std::printf(",");
        first = false;
        std::printf("{{\\\"case_id\\\":\\\"%s\\\",\\\"repetition\\\":%u,"
                    "\\\"elements\\\":%llu,\\\"input_digest\\\":\\\"%s\\\","
                    "\\\"status\\\":%d,\\\"output_bits\\\":",
                    item.id, item.repetition, item.elements, item.input_digest, status);
        if (status == 0) std::printf("%u,\\\"reorder_output_bits\\\":%u", output_bits, reorder_bits);
        else std::printf("null");
        std::printf("}}");
    }}
    std::printf("]}}\\n");
    return 0;
}}
"""


def run(arguments: list[str]) -> subprocess.CompletedProcess[bytes]:
    completed = subprocess.run(
        arguments,
        cwd=WORK_ROOT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.stderr:
        sys.stderr.buffer.write(completed.stderr)
    if completed.returncode != 0:
        if completed.stdout:
            sys.stderr.buffer.write(completed.stdout)
        raise SystemExit(completed.returncode)
    return completed


def main() -> None:
    bundle = json.loads(BUNDLE_PATH.read_text(encoding="utf-8"))
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    if bundle.get("schema_version") != 1 or bundle.get("role") not in {
        "cuda_reference",
        "ascend_candidate",
    }:
        fail("invalid trusted reduction execution bundle")
    callable_block = bundle.get("callable")
    if not isinstance(callable_block, dict) or not all(
        isinstance(callable_block.get(key), str) and IDENTIFIER.fullmatch(callable_block[key])
        for key in ("public_symbol", "reference_build_target", "candidate_build_target")
    ):
        fail("trusted reduction execution bundle carries no valid callable names")
    WORK_ROOT.mkdir(parents=True, exist_ok=True)
    TEMPORARY_PATH.mkdir(parents=True, exist_ok=True)
    HARNESS_PATH.write_text(harness_source(bundle, config), encoding="utf-8")
    source_root = "input" if bundle["role"] == "cuda_reference" else "generated"
    callable_names = bundle["callable"]
    target = (
        callable_names["reference_build_target"]
        if bundle["role"] == "cuda_reference"
        else callable_names["candidate_build_target"]
    )
    languages = "CXX CUDA" if bundle["role"] == "cuda_reference" else "CXX"
    cmake = f"""cmake_minimum_required(VERSION 3.24)
project(alloyport_reduction_correctness LANGUAGES {languages})
add_subdirectory({BUNDLE_ROOT / source_root} implementation-build)
add_executable(alloyport_reduction_harness {HARNESS_PATH})
target_compile_features(alloyport_reduction_harness PRIVATE cxx_std_17)
target_link_libraries(alloyport_reduction_harness PRIVATE {target})
"""
    (WORK_ROOT / "CMakeLists.txt").write_text(cmake, encoding="utf-8")
    run(["cmake", "-S", str(WORK_ROOT), "-B", str(BUILD_PATH)])
    run(["cmake", "--build", str(BUILD_PATH), "--target", "alloyport_reduction_harness", "--parallel", "1"])
    completed = run([str(BUILD_PATH / "alloyport_reduction_harness")])
    receipt = json.loads(completed.stdout)
    observations = receipt.get("observations")
    if (
        receipt.get("schema_version") != 1
        or receipt.get("experiment_digest") != bundle["experiment"]["experiment_digest"]
        or receipt.get("role") != bundle["role"]
        or receipt.get("candidate_id") != bundle["candidate_id"]
        or receipt.get("implementation_digest") != bundle["implementation_digest"]
        or receipt.get("corpus_digest") != bundle["experiment"]["corpus_digest"]
        or receipt.get("environment_digest") != config["environment_digest"]
        or receipt.get("implementation_invoked") is not True
        or receipt.get("synchronized") is not True
        or not isinstance(observations, list)
        or len(observations) != len(bundle["corpus"]["cases"])
    ):
        fail("trusted harness emitted a mismatched run receipt")
    for case, observation in zip(bundle["corpus"]["cases"], observations):
        if (
            observation.get("case_id") != case["case_id"]
            or observation.get("repetition") != case["repetition"]
            or observation.get("elements") != case["elements"]
            or observation.get("input_digest") != input_digest(case)
            or not isinstance(observation.get("status"), int)
            or (observation["status"] == 0) != isinstance(observation.get("output_bits"), int)
            or (observation["status"] == 0)
            != isinstance(observation.get("reorder_output_bits"), int)
        ):
            fail("trusted harness emitted a mismatched observation")
    sys.stdout.buffer.write(completed.stdout)


if __name__ == "__main__":
    main()
