{
  description = "queued_signal — Dioxus signal with wait-free reads and queued writes";

  inputs = {
    nixpkgs.url      = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url  = "github:numtide/flake-utils";
    wild = {
      url = "github:wild-linker/wild";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, wild, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [
          (import rust-overlay)
          (import wild)
        ];

        pkgs = import nixpkgs { inherit system overlays; };
        lib = pkgs.lib;

        rustStable = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-src" ];
        };

        wildStdenv = pkgs.useWildLinker pkgs.stdenv;
        mkShellWild = pkgs.mkShell.override { stdenv = wildStdenv; };

      in
      with pkgs;
      {
        devShells.default = mkShellWild {
          buildInputs = [
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
            xdotool
            zlib
            python3
            libxkbcommon
            vulkan-loader
            wayland
          ];
          env.RUSTFLAGS = "-C link-arg=-Wl,-rpath,${lib.makeLibraryPath (with pkgs; [ libxkbcommon vulkan-loader wayland ])}";
        };
      }
    ) // {
      nixConfig = {
        extra-substituters = [ "https://cache.nixos.org" ];
        extra-trusted-public-keys = [ "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=" ];
      };
    };
}
