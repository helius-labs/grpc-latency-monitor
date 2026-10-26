//! Subscription versions: the stored identity of each stream's measurement
//! configuration. Dashboards never mix results across versions.
use std::time::Duration;

use prost::Message;

use crate::{
    config::{FiltersConfig, ProviderConfig},
    model::{Commitment, DataType, StreamKind},
    storage::SubscriptionVersion,
    stream,
};

/// The ID covers everything that defines a measurement's meaning: the normalized
/// request, the on-time window and the compared providers. "Lag behind ideal" and
/// win shares are relative to every configured provider, so adding, removing,
/// renaming or re-pointing one starts a new configuration, and dashboards never mix
/// results measured under different definitions. Provider order and tokens are not
/// part of it (a rotated credential is the same measurement). Endpoint URLs can
/// embed credentials, so the stored description shows only a short hash of each.
pub fn subscription_versions(
    filters: &FiltersConfig,
    window: Duration,
    providers: &[ProviderConfig],
) -> [SubscriptionVersion; 6] {
    let filters = filters.normalized();
    let mut providers: Vec<(&str, &str)> = providers
        .iter()
        .map(|provider| {
            let url = provider.url.trim().trim_end_matches('/');
            (provider.name.as_str(), url)
        })
        .collect();
    providers.sort_unstable();
    let provider_set = providers
        .iter()
        .map(|(name, url)| format!("{name}@{}", &blake3::hash(url.as_bytes()).to_hex()[..8]))
        .collect::<Vec<_>>()
        .join(",");
    std::array::from_fn(|i| {
        let kind = StreamKind {
            data_type: DataType::ALL[i / 2],
            commitment: Commitment::ALL[i % 2],
        };
        // Hash the actual normalized protobuf request, never provider credentials.
        let request = stream::subscribe_request(kind, &filters);
        let mut hash = blake3::Hasher::new();
        hash.update(b"yellowstone-subscription-v2\0");
        hash.update(&request.encode_to_vec());
        hash.update(b"\0window-ns\0");
        hash.update(&window.as_nanos().to_be_bytes());
        hash.update(b"\0providers\0");
        for field in providers.iter().flat_map(|(name, url)| [name, url]) {
            hash.update(&(field.len() as u64).to_be_bytes());
            hash.update(field.as_bytes());
        }
        let description = match kind.data_type {
            DataType::Account => format!("accounts.owner={:?}", filters.accounts_owner),
            DataType::Transaction => format!(
                "transactions.account_include={:?};vote=all;failed=all",
                filters.transactions_account_include
            ),
            DataType::Slot => "slots.filter_by_commitment=false;interslot_updates=true".into(),
        };
        SubscriptionVersion {
            id: hash.finalize().to_hex().to_string(),
            filters: format!(
                "{};commitment={};window={};providers={provider_set};{description}",
                kind.data_type.as_str(),
                kind.commitment.as_str(),
                format_window(window)
            ),
        }
    })
}

/// Compact, exact rendering for configuration labels, e.g. "5s" or "1500ms".
fn format_window(window: Duration) -> String {
    if window.subsec_nanos() == 0 {
        format!("{}s", window.as_secs())
    } else if window.subsec_nanos().is_multiple_of(1_000_000) {
        format!("{}ms", window.as_millis())
    } else {
        format!("{}ns", window.as_nanos())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn normalized_ids_are_stable_and_distinct_without_credentials() {
        let config = Config::example();
        let filters = config.filters.clone();
        let providers = config.providers.clone();
        let window = Duration::from_secs(5);
        let versions = |filters: &FiltersConfig| subscription_versions(filters, window, &providers);
        let a = versions(&filters);
        assert_eq!(a[0].id, versions(&filters)[0].id);
        assert!(a.iter().all(|v| v.filters.contains(";window=5s;")));
        // A different on-time window is a different measurement for every stream.
        let other_window = subscription_versions(&filters, Duration::from_millis(1500), &providers);
        assert!(a.iter().zip(&other_window).all(|(a, b)| a.id != b.id));
        assert!(other_window[0].filters.contains(";window=1500ms;"));
        // The provider set is part of every stream's identity; order and tokens are not.
        let with =
            |providers: &[ProviderConfig]| subscription_versions(&filters, window, providers);
        let mut reordered_providers = providers.clone();
        reordered_providers.reverse();
        reordered_providers[0].token_env = Some("ROTATED_TOKEN".into());
        reordered_providers[1].url.push('/');
        assert_eq!(
            a.clone().map(|v| v.id),
            with(&reordered_providers).map(|v| v.id)
        );
        let mut repointed = providers.clone();
        repointed[1].url = "https://provider-b-eu.invalid:443".into();
        let mut renamed = providers.clone();
        renamed[1].name = "provider-c".into();
        let mut added = providers.clone();
        added.push(ProviderConfig {
            name: "provider-c".into(),
            url: "https://provider-c.invalid:443".into(),
            token_env: None,
        });
        for changed in [repointed, renamed, added] {
            assert!(a.iter().zip(&with(&changed)).all(|(a, b)| a.id != b.id));
        }
        // Names are shown; endpoints only as a short hash (URLs may hold credentials).
        let description = &a[0].filters;
        assert!(
            description.contains(";providers=provider-a@"),
            "{description}"
        );
        assert!(description.contains(",provider-b@"), "{description}");
        assert!(!description.contains("invalid"), "{description}");
        assert_eq!(
            a.iter()
                .map(|v| &v.id)
                .collect::<std::collections::HashSet<_>>()
                .len(),
            6
        );
        assert!(!a[0].filters.contains("token"));

        let mut reordered = filters.clone();
        reordered
            .accounts_owner
            .extend(filters.accounts_owner.clone());
        reordered
            .transactions_account_include
            .extend(filters.transactions_account_include.clone());
        reordered.accounts_owner.reverse();
        reordered.transactions_account_include.reverse();
        assert_eq!(a.map(|v| v.id), versions(&reordered).map(|v| v.id));

        let mut changed = filters;
        changed
            .accounts_owner
            .push("11111111111111111111111111111111".into());
        let b = versions(&changed);
        assert_ne!(b[0].id, versions(&reordered)[0].id);
        assert_ne!(b[1].id, versions(&reordered)[1].id);
        assert_eq!(b[2].id, versions(&reordered)[2].id);
        assert_eq!(b[4].id, versions(&reordered)[4].id);
    }
}
