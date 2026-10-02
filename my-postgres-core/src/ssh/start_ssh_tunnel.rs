use my_ssh::RemotePortForwardError;

use crate::PostgresConnectionString;

use super::PostgresSshConfig;

pub async fn start_ssh_tunnel_and_get_connection_string(
    connection_string: &mut PostgresConnectionString,
    ssh_config: &PostgresSshConfig,
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

    let (listen_host, listen_port) =
        crate::ssh::generate_unix_socket_file(ssh_config.credentials.as_ref());

    // A tunnel which has not started is not remembered, and the connection string keeps
    // the remote host - otherwise the next calls would find it as an established one
    ssh_session
        .start_port_forward_to_tcp(
            format!("{}:{}", listen_host, listen_port),
            connection_string.get_host().to_string(),
            connection_string.get_port(),
        )
        .await?;

    {
        let mut tunnels_access = crate::ssh::ESTABLISHED_TUNNELS.lock().await;
        tunnels_access.insert(ssh_tunnel_key, (listen_host.to_string(), listen_port));
    }

    connection_string.set_host(listen_host.to_string().to_string());
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

    use super::start_ssh_tunnel_and_get_connection_string;

    const SSH_LINE: &str = "user@10.0.0.5:22";
    const TUNNEL_KEY: &str = "10.0.0.5:22->10.0.0.10:5432";

    fn get_conn_string() -> PostgresConnectionString {
        PostgresConnectionString::from_str(
            "host=10.0.0.10 port=5432 dbname=db user=usr password=pwd ssh=user@10.0.0.5:22",
        )
    }

    async fn get_established_tunnel() -> Option<(String, u16)> {
        ESTABLISHED_TUNNELS.lock().await.get(TUNNEL_KEY).cloned()
    }

    // Starting a tunnel only binds the local port - the ssh session is opened by the first
    // connection which comes to that port. So no ssh server is needed here.
    #[tokio::test]
    async fn test_tunnel_which_failed_to_start_is_not_remembered() {
        let ssh_config = SshConfigBuilder::new().build(SSH_LINE);

        // Ports are given out one by one, so the next one is known and gets taken before
        // the tunnel asks for it. If it can not be taken here - somebody else holds it,
        // which is the same for the tunnel.
        let busy_port = PORT_ALLOCATOR.get_next_port() + 1;
        let _busy_port_listener = TcpListener::bind(("127.0.0.1", busy_port)).await;

        let mut conn_string = get_conn_string();
        let result =
            start_ssh_tunnel_and_get_connection_string(&mut conn_string, &ssh_config).await;

        assert!(result.is_err());
        assert_eq!("10.0.0.10", conn_string.get_host());
        assert_eq!(5432, conn_string.get_port());
        assert_eq!(None, get_established_tunnel().await);

        // The next attempts go to the next ports - the first free one gets the tunnel
        let mut attempts = 0;
        while start_ssh_tunnel_and_get_connection_string(&mut conn_string, &ssh_config)
            .await
            .is_err()
        {
            attempts += 1;
            assert!(attempts < 100);
        }

        let tunnel_port = conn_string.get_port();

        assert_eq!("127.0.0.1", conn_string.get_host());
        assert_ne!(busy_port, tunnel_port);
        assert_eq!(
            Some(("127.0.0.1".to_string(), tunnel_port)),
            get_established_tunnel().await
        );

        // The tunnel holds its port
        assert!(TcpListener::bind(("127.0.0.1", tunnel_port)).await.is_err());

        // The established tunnel is reused
        let mut next_conn_string = get_conn_string();
        start_ssh_tunnel_and_get_connection_string(&mut next_conn_string, &ssh_config)
            .await
            .unwrap();

        assert_eq!("127.0.0.1", next_conn_string.get_host());
        assert_eq!(tunnel_port, next_conn_string.get_port());
    }
}
