{
  description = "A very basic flake";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
  };

   outputs = { self, nixpkgs, ... }:
    let
     system = "aarch64-darwin"; # your version
     pkgs = nixpkgs.legacyPackages.${system};    
    in
    {
      devShells.${system}.default = pkgs.mkShell
      {
        nativeBuildInputs = with pkgs; [
          binaryen
          flyctl
          rustup
          openssl
          protobuf_21
        ] ++ (if pkgs.stdenv.isDarwin then [
            darwin.apple_sdk.frameworks.Foundation
            pkgs.darwin.libiconv
          ] else []);

        shellHook = ''
          ./setup_git_hook.sh
        '';
      };
    };
}
