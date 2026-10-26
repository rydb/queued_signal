{
  description = "queued_signal development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
    wild = {
      url = "github:wild-linker/wild";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, wild, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        # Dioxus CLI built from the published crate.
        dioxus-cli-overlay = final: prev: {
          dioxus-cli = prev.rustPlatform.buildRustPackage {
            pname = "dioxus-cli";
            version = "0.8.0-alpha.3";
            src = prev.fetchCrate {
              pname = "dioxus-cli";
              version = "0.8.0-alpha.3";
              hash = "sha256-n1Lk9eCjr1vGqvCDbrOmbU7TTvdcGdx/chRsj4Z3a+Q=";
            };
            cargoHash = "sha256-nqGrW0Sf7DWdrSXSKcSl+FlO+1NY4uZA7G7G4AGItuw=";
            buildFeatures = [ "no-downloads" "disable-telemetry" ];
            nativeBuildInputs = [
              prev.pkg-config
              prev.cacert
              prev.makeWrapper
            ];
            buildInputs = [ prev.openssl ];
            nativeCheckInputs = [ prev.rustfmt ];
            checkFlags = [ "--skip=test_harnesses::run_harness" ];
            env.OPENSSL_NO_VENDOR = 1;
            postFixup = ''
              wrapProgram $out/bin/dx \
                --suffix PATH : ${prev.lib.makeBinPath [ prev.esbuild prev.wasm-bindgen-cli_0_2_128 ]}
            '';
            meta = with prev.lib; {
              description = "CLI for building fullstack web, desktop, and mobile apps with a single codebase.";
              homepage = "https://dioxus.dev";
              license = with licenses; [ mit asl20 ];
              mainProgram = "dx";
            };
          };
        };

        overlays = [
          (import rust-overlay)
          (import wild)
          dioxus-cli-overlay
        ];

        pkgs = import nixpkgs { inherit system overlays; };
        lib = pkgs.lib;

        rustStable = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-src" "rust-analyzer" ];
          targets = [ "wasm32-unknown-unknown" ];
        };

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
          vulkan-tools
          wayland
          fontconfig
          freetype
          dejavu_fonts
          xdotool
          dioxus-cli
        ];

        commonRpath = lib.makeLibraryPath (with pkgs; [
          libxkbcommon
          vulkan-loader
          wayland
        ]);

        wildStdenv = pkgs.useWildLinker pkgs.stdenv;
        mkShellWild = pkgs.mkShell.override { stdenv = wildStdenv; };

      in
      {
        devShells.default = mkShellWild {
          buildInputs = commonBuildInputs;
          env.RUSTFLAGS = "-C link-arg=-Wl,-rpath,${commonRpath}";
          shellHook = ''
            export PATH="$HOME/.cargo/bin:$PATH"
          '';
        };
      }
    );
}
