// Copyright 2026 The Kubetail Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Kube-context domain logic for the "Default Context" tray feature.
//!
//! Pure, dependency-light helpers: tray menu-id formatting/parsing and the
//! per-context descriptor list. The tray/menu wiring that consumes these — and
//! the gRPC watch/set RPCs that feed them — lives in the parent [`super`]
//! module (`tray`). Everything here is unit-tested.

/// Prefix for dynamic tray menu item ids, e.g. `kube_ctx::minikube`. The
/// context name follows verbatim — `parse_context_menu_id` strips exactly
/// this prefix, so names containing `::` round-trip correctly.
const CTX_ID_PREFIX: &str = "kube_ctx::";

/// A tray menu item descriptor for one kube-context. The menu id is derived
/// from `label` on demand via [`context_menu_id`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextItem {
    pub label: String,
    pub checked: bool,
}

/// Formats the tray menu id for a context name.
pub fn context_menu_id(name: &str) -> String {
    format!("{CTX_ID_PREFIX}{name}")
}

/// Returns the context name for a kube-context menu id, or `None` for any
/// other menu id (e.g. `tray_quit`).
pub fn parse_context_menu_id(id: &str) -> Option<&str> {
    id.strip_prefix(CTX_ID_PREFIX)
}

/// Builds the per-context menu descriptors from a context list and the
/// current context. Sorted by name for a stable menu order; exactly the
/// entry matching `current` is checked (none, if `current` isn't present).
pub fn build_context_descriptors(contexts: &[String], current: &str) -> Vec<ContextItem> {
    let mut names: Vec<&String> = contexts.iter().collect();
    names.sort();
    names
        .into_iter()
        .map(|name| ContextItem {
            label: name.clone(),
            checked: name == current,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn menu_id_round_trips() {
        let id = context_menu_id("minikube");
        assert_eq!(id, "kube_ctx::minikube");
        assert_eq!(parse_context_menu_id(&id), Some("minikube"));
    }

    #[test]
    fn menu_id_round_trips_with_colons_in_name() {
        // Names with `::` (e.g. EKS arns) must survive the round-trip.
        let name = "arn:aws:eks:us-east-1::cluster/foo";
        let id = context_menu_id(name);
        assert_eq!(parse_context_menu_id(&id), Some(name));
    }

    #[test]
    fn parse_rejects_non_context_ids() {
        assert_eq!(parse_context_menu_id("tray_quit"), None);
        assert_eq!(parse_context_menu_id("new_window"), None);
    }

    #[test]
    fn descriptors_check_current_and_sort() {
        let contexts = vec!["prod".to_string(), "dev".to_string(), "stage".to_string()];
        let items = build_context_descriptors(&contexts, "stage");
        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert_eq!(labels, vec!["dev", "prod", "stage"]); // sorted
        let checked: Vec<&str> = items
            .iter()
            .filter(|i| i.checked)
            .map(|i| i.label.as_str())
            .collect();
        assert_eq!(checked, vec!["stage"]); // exactly the current one
        assert_eq!(context_menu_id(&items[0].label), "kube_ctx::dev");
    }

    #[test]
    fn descriptors_empty_list_is_empty() {
        assert!(build_context_descriptors(&[], "anything").is_empty());
    }

    #[test]
    fn descriptors_current_not_in_list_checks_none() {
        let contexts = vec!["a".to_string(), "b".to_string()];
        let items = build_context_descriptors(&contexts, "missing");
        assert!(items.iter().all(|i| !i.checked));
    }
}
