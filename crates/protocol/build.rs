//! Compile the protobuf definitions with a vendored `protoc`, so that no
//! system installation is required.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    // Byte fields become `bytes::Bytes` so that large payloads are not
    // copied into `Vec<u8>` on decode.
    config.bytes(["."]);
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_with_config(
            config,
            &["proto/arca_lake/storage/v1/storage.proto"],
            &["proto"],
        )?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
