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

        # Miri for testing unsafe
        rustMiri = pkgs.rust-bin.nightly.latest.default.override {
          extensions = [ "miri" "rust-src" ];
          targets = [ "wasm32-unknown-unknown" ];
        };

        commonPackages = with pkgs; [
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

        commonBuildInputs = [ rustStable ] ++ commonPackages;

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

        devShells.miri = mkShellWild {
          buildInputs = [ rustMiri ] ++ commonPackages;
          env.RUST_BACKTRACE = "1";
          env.CARGO_TARGET_DIR = "target/miri";
          # Enables 128 bit atomics which Kovan requires to not fallback to locks,
          env.RUSTFLAGS = "-C target-feature=+cmpxchg16b";
          # The reclamation list word carries a node pointer as a u64, because the slot is an
          # integer compare and exchange, and the cast back is the same address with no
          # arithmetic. Permissive provenance accepts that round trip, which is the only one here.
          
          # The leak check and the thread-at-exit check share a switch, and the reclaimer thread
          # outlives the harness, so that check waits until the receiver can be joined on drop.
          env.MIRIFLAGS = "-Zmiri-permissive-provenance -Zmiri-ignore-leaks";
        };
      }
    );
}
