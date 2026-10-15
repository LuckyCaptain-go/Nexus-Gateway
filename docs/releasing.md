# Release Process

## Current automation

The Go workflow checks pull requests and pushes to `master`. It does not publish binaries or container images. GitHub Releases are currently created by a maintainer.

## Release checklist

1. Confirm the scope and resolve blocking security or compatibility issues.
2. From a clean checkout, run:
   ```bash
   go vet ./...
   go test ./...
   go build ./...
   ```
   Also confirm changed Go files are formatted with `gofmt`.
3. Move relevant `[Unreleased]` entries in [CHANGELOG.md](../CHANGELOG.md) under a new `vX.Y.Z` heading and date. Call out breaking changes and migration steps.
4. Check that README, configuration, and supported-source claims match the release code.
5. Create and push an annotated tag, for example `git tag -a v1.3.0 -m "Release v1.3.0"` and `git push origin v1.3.0`.
6. Create a GitHub Release with highlights, fixes, known limitations, and a changelog link.
7. Verify the release page and tag point to the intended `master` commit.

## Container images

Container publishing is not configured. The Docker build definition needs alignment with the Go version and runtime configuration (tracked in [issue #30](https://github.com/RhinoLink-Tech/data-fabric/issues/30)). Do not advertise an image as a supported release artifact until that issue is resolved and the image is built and smoke-checked.
