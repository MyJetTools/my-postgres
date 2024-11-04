use crate::PostgresConnectionString;

use super::PostgresSshConfig;

pub async fn start_ssh_tunnel_and_get_connection_string(
    connection_string: &mut PostgresConnectionString,
    ssh_config: &PostgresSshConfig,
) {
    let (host, port) = ssh_config.credentials.get_host_port();

    let ssh_tunnel_key = format!(
        "{}:{}->{}:{}",
        host,
        port,
        connection_string.get_host(),
        connection_string.get_port()
    );

    {
        let tunnels_access = crate::ssh::ESTABLISHED_TUNNELS.lock().await;

        if let Some((local_host, local_port)) = tunnels_access.get(ssh_tunnel_key.as_str()) {
            connection_string.set_host(local_host.to_string());
            connection_string.set_port(*local_port);
            return;
        }
    }

    let ssh_session = ssh_config.get_ssh_session().await;

    let (listen_host, listen_port) =
        crate::ssh::generate_unix_socket_file(ssh_config.credentials.as_ref());

    let result = ssh_session
        .start_port_forward(
            format!("{}:{}", listen_host, listen_port),
            connection_string.get_host().to_string(),
            connection_string.get_port(),
        )
        .await;

    if let Err(result) = result {
        println!("Can not start port forwarding with error: {:?}", result);
    }

    {
        let mut tunnels_access = crate::ssh::ESTABLISHED_TUNNELS.lock().await;
        tunnels_access.insert(ssh_tunnel_key, (listen_host.to_string(), listen_port));
    }

    connection_string.set_host(listen_host.to_string().to_string());
    connection_string.set_port(listen_port);
}
