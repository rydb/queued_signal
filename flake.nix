{
  description = "project flake";

  inputs = {
    nixpkgs.url      = "github:NixOS/nixpkgs/nixpkgs-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url  = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [
          (import rust-overlay)
        ];

        pkgs = import nixpkgs { inherit system overlays; };
        lib = pkgs.lib;

        rustStable = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-src" ];
        };

        wildStdenv = pkgs.useWildLinker pkgs.stdenv;
        mkShellWild = pkgs.mkShell.override { stdenv = wildStdenv; };

        commonBuildInputs = with pkgs; [
          rustStable
          gcc
          systemd
          alsa-lib
          pkg-config
          binutils
          gnumake
          openssl
          glib
          pango
          gdk-pixbuf
          cairo
          atk
          gtk3
          webkitgtk_4_1
          zlib
          libxkbcommon
          vulkan-loader
          wayland
        ];

        commonRpath = lib.makeLibraryPath (with pkgs; [
          libxkbcommon
          vulkan-loader
          wayland
        ]);

      in
      with pkgs;
      {
        devShells.default = mkShellWild {
          buildInputs = commonBuildInputs ++ [ xdotool python3 ];
          env.RUSTFLAGS = "-C link-arg=-Wl,-rpath,${commonRpath}";
        };
      }
    ) // {
      nixConfig = {
        extra-substituters = [ "https://cache.nixos.org" ];
        extra-trusted-public-keys = [ "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=" ];
      };
    };
}
