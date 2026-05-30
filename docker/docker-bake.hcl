# ═══════════════════════════════════════════════════════════════════════════
# Kora Docker Buildx Bake Configuration
# ═══════════════════════════════════════════════════════════════════════════

variable "REGISTRY" {
  default = "ghcr.io/refcell/kora"
}

variable "PLATFORMS" {
  default = "linux/amd64,linux/arm64"
}

variable "GIT_REF_NAME" {
  default = "main"
}

variable "BUILD_PROFILE" {
  default = "release"
}

# Git metadata for image labels. CI populates these from github context;
# local builds fall back to auto-detection via the Justfile/shell.
variable "GIT_SHA" {
  default = ""
}

variable "GIT_SHA_SHORT" {
  default = ""
}

variable "BUILD_TIMESTAMP" {
  default = ""
}

variable "SEMVER" {
  default = ""
}

# ───────────────────────────────────────────────────────────────────────────
# Groups
# ───────────────────────────────────────────────────────────────────────────

group "default" {
  targets = ["kora"]
}

group "all" {
  targets = ["kora", "kora-dev"]
}

# ───────────────────────────────────────────────────────────────────────────
# Tagging strategy:
#   - Every build: <registry>:sha-<short-sha>
#   - Branch builds: <registry>:<branch-name>
#   - Tagged releases: <registry>:<semver>, <registry>:latest
# ───────────────────────────────────────────────────────────────────────────

function "image_tags" {
  params = [name]
  result = compact([
    # Always tag with branch/ref name
    "${REGISTRY}/${name}:${GIT_REF_NAME}",
    # Tag with short SHA when available (immutable reference)
    GIT_SHA_SHORT != "" ? "${REGISTRY}/${name}:sha-${GIT_SHA_SHORT}" : "",
    # Tag with semver when building a release
    SEMVER != "" ? "${REGISTRY}/${name}:${SEMVER}" : "",
    # Tag latest only for semver releases
    SEMVER != "" ? "${REGISTRY}/${name}:latest" : "",
  ])
}

function "oci_labels" {
  params = []
  result = {
    "org.opencontainers.image.source"   = "https://github.com/refcell/kora"
    "org.opencontainers.image.revision" = GIT_SHA
    "org.opencontainers.image.created"  = BUILD_TIMESTAMP
    "org.opencontainers.image.version"  = SEMVER != "" ? SEMVER : GIT_SHA_SHORT
  }
}

# ───────────────────────────────────────────────────────────────────────────
# Base target for shared configuration
# ───────────────────────────────────────────────────────────────────────────

target "docker-metadata-action" {
  tags   = image_tags("kora")
  labels = oci_labels()
}

# ───────────────────────────────────────────────────────────────────────────
# Production build - multi-platform
# ───────────────────────────────────────────────────────────────────────────

target "kora" {
  inherits   = ["docker-metadata-action"]
  context    = ".."
  dockerfile = "docker/Dockerfile"
  platforms  = split(",", PLATFORMS)
  args = {
    BUILD_PROFILE   = BUILD_PROFILE
    GIT_SHA         = GIT_SHA
    GIT_SHA_SHORT   = GIT_SHA_SHORT
    BUILD_TIMESTAMP = BUILD_TIMESTAMP
  }
}

# ───────────────────────────────────────────────────────────────────────────
# Local development build - single platform, local only
# ───────────────────────────────────────────────────────────────────────────

target "kora-local" {
  context    = ".."
  dockerfile = "docker/Dockerfile"
  platforms  = ["linux/amd64"]
  tags       = compact([
    "kora:local",
    GIT_SHA_SHORT != "" ? "kora:sha-${GIT_SHA_SHORT}" : "",
  ])
  labels = oci_labels()
  args = {
    BUILD_PROFILE   = "release"
    GIT_SHA         = GIT_SHA
    GIT_SHA_SHORT   = GIT_SHA_SHORT
    BUILD_TIMESTAMP = BUILD_TIMESTAMP
  }
}

# ───────────────────────────────────────────────────────────────────────────
# Development build with debug symbols
# ───────────────────────────────────────────────────────────────────────────

target "kora-dev" {
  context    = ".."
  dockerfile = "docker/Dockerfile"
  platforms  = ["linux/amd64"]
  tags       = compact([
    "kora:dev",
    GIT_SHA_SHORT != "" ? "kora:dev-sha-${GIT_SHA_SHORT}" : "",
  ])
  labels = oci_labels()
  args = {
    BUILD_PROFILE   = "dev"
    GIT_SHA         = GIT_SHA
    GIT_SHA_SHORT   = GIT_SHA_SHORT
    BUILD_TIMESTAMP = BUILD_TIMESTAMP
  }
}
