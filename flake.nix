{
  description = "A modern, user-friendly tool for interacting with Qualcomm devices in Emergency Download (EDL) mode";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    systems.url = "github:nix-systems/default";
  };

  outputs = { self, ... }@inputs:
    let
      eachSystem = inputs.nixpkgs.lib.genAttrs (import inputs.systems);
    in
    {
      packages = eachSystem (system: {
        edl-ng = inputs.nixpkgs.legacyPackages.${system}.callPackage ./pkgs/edl-ng { };
        default = self.packages.${system}.edl-ng;
      });

      devShells = eachSystem (system: {
        default =
          inputs.nixpkgs.legacyPackages.${system}.mkShellNoCC {
            name = "edl-ng-shell";
            inputsFrom = [
              self.packages.${system}.default
            ];
          };
      });

      formatter = eachSystem (system: inputs.nixpkgs.legacyPackages.${system}.nixpkgs-fmt);
    };
}
