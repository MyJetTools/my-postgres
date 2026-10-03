use my_ssh::RemotePortForwardError;

use crate::PostgresConnectionString;

use super::PostgresSshConfig;

// How many local ports one call tries before it gives up. Every process starts giving out ports
// from the same one, so a port taken by another service on the machine is an ordinary thing.
const MAX_LISTEN_PORT_ATTEMPTS: usize = 100;

pub async fn start_ssh_tunnel_and_get_connection_string(
    connection_string: &mut PostgresConnectionString,
    ssh_config: &PostgresSshConfig,
) -> Result<(), RemotePortForwardError> {
    start_ssh_tunnel(
        connection_string,
        ssh_config,
        || crate::ssh::generate_unix_socket_file(ssh_config.credentials.as_ref()),
        MAX_LISTEN_PORT_ATTEMPTS,
    )
    .await
}

async fn start_ssh_tunnel(
    connection_string: &mut PostgresConnectionString,
    ssh_config: &PostgresSshConfig,
    mut next_listen_endpoint: impl FnMut() -> (&'static str, u16),
    max_attempts: usize,
) -> Result<(), RemotePortForwardError> {
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
            return Ok(());
        }
    }

    let ssh_session = ssh_config.get_ssh_session().await;

    let mut attempt = 0;

    let (listen_host, listen_port) = loop {
        attempt += 1;

        let (listen_host, listen_port) = next_listen_endpoint();

        let result = ssh_session
            .start_port_forward_to_tcp(
                format!("{}:{}", listen_host, listen_port),
                connection_string.get_host().to_string(),
                connection_string.get_port(),
            )
            .await;

        match result {
            Ok(_) => break (listen_host, listen_port),
            // The local port is taken - the next one is tried straight away
            Err(RemotePortForwardError::CanNotBindListenEndpoint(err)) => {
                if attempt >= max_attempts {
                    return Err(RemotePortForwardError::CanNotBindListenEndpoint(format!(
                        "No local port to listen on after {} attempts. The last one: {}",
                        attempt, err
                    )));
                }
            }
            // A tunnel which has not started is not remembered, and the connection string keeps
            // the remote host - otherwise the next calls would find it as an established one
            Err(err) => return Err(err),
        }
    };

    {
        let mut tunnels_access = crate::ssh::ESTABLISHED_TUNNELS.lock().await;
        tunnels_access.insert(ssh_tunnel_key, (listen_host.to_string(), listen_port));
    }

    connection_string.set_host(listen_host.to_string());
    connection_string.set_port(listen_port);

    Ok(())
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;

    use crate::{
        ssh::{SshConfigBuilder, ESTABLISHED_TUNNELS, PORT_ALLOCATOR},
        PostgresConnectionString,
    };

    use super::{start_ssh_tunnel, start_ssh_tunnel_and_get_connection_string};

    const SSH_LINE: &str = "user@10.0.0.5:22";

    fn get_conn_string(postgres_host: &str) -> PostgresConnectionString {
        PostgresConnectionString::from_str(
            format!(
                "host={} port=5432 dbname=db user=usr password=pwd ssh=user@10.0.0.5:22",
                postgres_host
            )
            .as_str(),
        )
    }

    async fn get_established_tunnel(postgres_host: &str) -> Option<(String, u16)> {
        let tunnel_key = format!("10.0.0.5:22->{}:5432", postgres_host);
        ESTABLISHED_TUNNELS.lock().await.get(&tunnel_key).cloned()
    }

    // Starting a tunnel only binds the local port - the ssh session is opened by the first
    // connection which comes to that port. So no ssh server is needed here.
    #[tokio::test]
    async fn test_busy_local_port_is_skipped_within_one_call() {
        let ssh_config = SshConfigBuilder::new().build(SSH_LINE);

        // Ports are given out one by one, so the next one is known and gets taken before
        // the tunnel asks for it. If it can not be taken here - somebody else holds it,
        // which is the same for the tunnel.
        let busy_port = PORT_ALLOCATOR.get_next_port() + 1;
        let _busy_port_listener = TcpListener::bind(("127.0.0.1", busy_port)).await;

        let mut conn_string = get_conn_string("10.0.0.10");
        start_ssh_tunnel_and_get_connection_string(&mut conn_string, &ssh_config)
            .await
            .unwrap();

        let tunnel_port = conn_string.get_port();

        assert_eq!("127.0.0.1", conn_string.get_host());
        assert_ne!(busy_port, tunnel_port);
        assert_eq!(
            Some(("127.0.0.1".to_string(), tunnel_port)),
            get_established_tunnel("10.0.0.10").await
        );

        // The tunnel holds its port
        assert!(TcpListener::bind(("127.0.0.1", tunnel_port)).await.is_err());

        // The established tunnel is reused
        let mut next_conn_string = get_conn_string("10.0.0.10");
        start_ssh_tunnel_and_get_connection_string(&mut next_conn_string, &ssh_config)
            .await
            .unwrap();

        assert_eq!("127.0.0.1", next_conn_string.get_host());
        assert_eq!(tunnel_port, next_conn_string.get_port());
    }

    #[tokio::test]
    async fn test_tunnel_which_failed_to_start_is_not_remembered() {
        let ssh_config = SshConfigBuilder::new().build(SSH_LINE);

        // A port held here is the only one the tunnel is offered, so every attempt fails
        let busy_port_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let busy_port = busy_port_listener.local_addr().unwrap().port();

        let mut offered = 0;
        let mut conn_string = get_conn_string("10.0.0.11");
        let result = start_ssh_tunnel(
            &mut conn_string,
            &ssh_config,
            || {
                offered += 1;
                ("127.0.0.1", busy_port)
            },
            3,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(3, offered);
        assert_eq!("10.0.0.11", conn_string.get_host());
        assert_eq!(5432, conn_string.get_port());
        assert_eq!(None, get_established_tunnel("10.0.0.11").await);
    }
}
