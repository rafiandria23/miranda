use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "../../proto/miranda/worker/v1/worker.proto",
                "../../proto/miranda/control_plane/v1/control_plane.proto",
            ],
            &["../../proto"],
        )?;

    Ok(())
}
