
// use anyhow::Context;
// use rcgen::generate_simple_self_signed;
// use rustls::{ClientConfig, RootCertStore, ServerConfig};
// use rustls_pki_types::{CertificateDer, PrivateKeyDer};
// use sha2::{Digest, Sha256};
// use std::{path::Path, sync::Arc};

// pub struct Identity {
//     pub cert_der: Vec<u8>,
//     pub key_der: Vec<u8>,
// }

// pub fn cert_fingerprint_sha256_hex(cert_der: &[u8]) -> String {
//     let mut h = Sha256::new();
//     h.update(cert_der);
//     hex::encode(h.finalize())
// }

// pub fn load_or_create_identity(identity_dir: &Path) -> anyhow::Result<Identity> {
//     std::fs::create_dir_all(identity_dir)?;

//     let cert_path = identity_dir.join("cert.der");
//     let key_path = identity_dir.join("key.der");

//     if cert_path.exists() && key_path.exists() {
//         let cert_der = std::fs::read(&cert_path)?;
//         let key_der = std::fs::read(&key_path)?;
//         return Ok(Identity { cert_der, key_der });
//     }

//     let subject_alt_names = vec!["vault-syncd.local".to_string()];
//     let cert = generate_simple_self_signed(subject_alt_names)?;
//     let cert_der = cert.serialize_der()?;
//     let key_der = cert.serialize_private_key_der();

//     std::fs::write(&cert_path, &cert_der)?;
//     std::fs::write(&key_path, &key_der)?;

//     Ok(Identity { cert_der, key_der })
// }

// pub fn server_config(identity: &Identity) -> anyhow::Result<Arc<ServerConfig>> {
//     let cert = CertificateDer::from(identity.cert_der.clone());
//     let key = PrivateKeyDer::try_from(identity.key_der.clone())
//         .context("failed to parse private key")?;

//     let cfg = ServerConfig::builder()
//         .with_no_client_auth()
//         .with_single_cert(vec![cert], key)?;

//     Ok(Arc::new(cfg))
// }

// pub fn client_config(identity: &Identity, pinned_peer_sha256_hex: &str) -> anyhow::Result<Arc<ClientConfig>> {
//     // No usamos WebPKI/CA pública. Verificamos por pinning post-handshake (ver main).
//     // Aun así, necesitamos un RootCertStore: lo dejamos vacío.
//     let roots = RootCertStore::empty();

//     let cert = CertificateDer::from(identity.cert_der.clone());
//     let key = PrivateKeyDer::try_from(identity.key_der.clone())
//         .context("failed to parse private key")?;

//     let cfg = ClientConfig::builder()
//         .with_root_certificates(roots)
//         .with_client_auth_cert(vec![cert], key)?;

//     // OJO: La verificación por pinning la hacemos manualmente leyendo el cert presentado.
//     // Aquí dejamos el TLS “confiar en cualquiera” para poder conectar en ZeroTier,
//     // y validamos inmediatamente después (si pin no coincide => cerrar).
//     // Para eso necesitamos un custom verifier; lo haremos en el siguiente paso si quieres “100% formal”.
//     // Por ahora: pinning manual + cierre inmediato.
//     // todo esto es experimmenatl en si 

//     let _ = pinned_peer_sha256_hex; // usado en main para comparar
//     Ok(Arc::new(cfg))
// }