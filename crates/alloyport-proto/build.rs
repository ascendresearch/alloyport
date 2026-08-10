use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let mut prost = prost_build::Config::new();
    prost.protoc_executable(protoc);

    tonic_prost_build::configure().compile_with_config(
        prost,
        &[
            "proto/alloyport/worker/v1/worker_control.proto",
            "proto/alloyport/artifact/v1/artifact_service.proto",
        ],
        &["proto"],
    )?;
    println!("cargo:rerun-if-changed=proto/alloyport/worker/v1/worker_control.proto");
    println!("cargo:rerun-if-changed=proto/alloyport/artifact/v1/artifact_service.proto");
    Ok(())
}
