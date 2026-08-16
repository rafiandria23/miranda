## 🔗 Related Issues
<!-- List all related Jira tickets and GitHub issues. -->
<!-- For GitHub, use keywords like 'Closes', 'Fixes', or 'Resolves' to auto-close the issue on merge. -->

**Jira Tickets:**

- [RF-XXXX](https://rafiandria23.atlassian.net/browse/RF-XXXX)
- [RF-XXXX](https://rafiandria23.atlassian.net/browse/RF-XXXX)

**GitHub Issues:**

- Closes #XXXX
- Ref #XXXX

## 📝 Description

**What does this PR do?**
<!-- Describe the technical implementation of the change. -->

**Why is this change necessary?**
<!-- Explain the context. If the tickets cover this entirely, you can keep this brief, but highlight any specific technical decisions made. -->

## 🦀 Rust Quality Checklist
<!-- Ensure these steps are complete before requesting a review to save CI time. -->
- [ ] Formatted code: `cargo fmt --all`
- [ ] Passed lints: `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] Passed tests: `cargo test --workspace --all-features`
- [ ] If storage code changed: migrations run and offline query cache is in sync (`make db-migrate-all` then `cargo sqlx prepare --check` in the affected `storage-*` crate)
- [ ] Added or updated documentation (if applicable)

<!-- CI also runs SonarCloud and Codecov automatically on this PR — no local action needed for those. -->

## 🛠️ Type of Change

- [ ] 🐛 Bug fix (non-breaking change which fixes an issue)
- [ ] ✨ New feature (non-breaking change which adds functionality)
- [ ] 💥 Breaking change (fix or feature that would cause existing functionality to fail)
- [ ] ♻️ Refactor / Performance / Infrastructure
