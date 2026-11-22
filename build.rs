fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use the system protoc when PROTOC isn't set (Homebrew installs it here on macOS).
    if std::env::var_os("PROTOC").is_none() {
        for p in ["/opt/homebrew/bin/protoc", "/usr/local/bin/protoc", "/usr/bin/protoc"] {
            if std::path::Path::new(p).exists() {
                // SAFETY: build scripts are single-threaded at this point.
                unsafe { std::env::set_var("PROTOC", p) };
                break;
            }
        }
    }
    println!("cargo:rerun-if-changed=proto");
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .generate_default_stubs(true)
        .include_file("temporal_protos.rs")
        .compile_protos(
            &[
                "proto/temporal/api/workflowservice/v1/service.proto",
                "proto/temporal/api/operatorservice/v1/service.proto",
                "proto/temporal/api/errordetails/v1/message.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
