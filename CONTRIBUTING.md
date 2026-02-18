# Contributing to IncidentBench

Thank you for your interest in contributing to IncidentBench! This document explains how to get started.

## Developer Certificate of Origin (DCO)

This project uses the [Developer Certificate of Origin (DCO)](https://developercertificate.org/) instead of a Contributor License Agreement (CLA). By submitting a contribution, you certify that you have the right to do so under the DCO.

Every commit must be signed off with your real name and email address:

```
Signed-off-by: Your Name <your.email@example.com>
```

You can do this automatically by committing with the `-s` flag:

```bash
git commit -s -m "Add new feature"
```

If you've already made commits without the sign-off, you can amend the most recent one:

```bash
git commit --amend -s
```

Or rebase to sign off multiple commits:

```bash
git rebase --signoff HEAD~<number-of-commits>
```

> **Note**: The sign-off must match the author information on the commit. Using a pseudonym or anonymous contributions are not accepted.

### Developer Certificate of Origin v1.1

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.

Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

## How to Contribute

### Reporting Issues

- Use [GitHub Issues](https://github.com/mach5-io/IncidentBench/issues) to report bugs or request features.
- Search existing issues first to avoid duplicates.
- For bugs, include: steps to reproduce, expected behavior, actual behavior, and environment details (Rust version, Kubernetes version, OS).

### Submitting Changes

1. **Fork** the repository and create a branch from `master`:
   ```bash
   git checkout -b my-feature master
   ```

2. **Make your changes**. Follow the existing code style and conventions.

3. **Test your changes**:
   ```bash
   make build          # Ensure it compiles
   cargo test          # Run the test suite
   cargo clippy        # Check for lint warnings
   cargo fmt --check   # Check formatting
   ```

4. **Commit with DCO sign-off**:
   ```bash
   git commit -s -m "Describe your change"
   ```

5. **Push** your branch and open a **Pull Request** against `master`.

### Pull Request Guidelines

- Keep PRs focused on a single change.
- Write a clear description of what the PR does and why.
- Ensure CI passes before requesting review.
- Be responsive to review feedback.

## Development Setup

### Prerequisites

- Rust 1.75+
- Docker
- A Kubernetes cluster (v1.28+) or `kind` for local development
- `kubectl` configured for your cluster
- `protoc` (Protocol Buffers compiler)

### Building

```bash
# Native build
make build

# Docker images
make docker-build

# Full local dev environment (kind + Kafka + operator)
make deploy-local
```

### Project Structure

```
crates/
├── incidentbench-common     # Shared types: CRD, scenario, adapter trait, metrics
├── incidentbench-operator   # Kubernetes operator (reconciler + resource builders)
├── incidentbench-worker     # Worker binary (ingest, query, phase-controller, aggregator)
├── incidentbench-reporter   # Report generator
└── incidentbench-cli        # CLI tool
```

## Code Style

- Follow standard Rust conventions and idioms.
- Use `cargo fmt` to format code before committing.
- Use `cargo clippy` to catch common mistakes.
- Write meaningful commit messages that explain *why*, not just *what*.

## License

By contributing to IncidentBench, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
