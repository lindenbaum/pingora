{
  outputs = {nixpkgs,...}: 
  let
  pkgs = nixpkgs.legacyPackages.x86_64-linux;
  in
  {
    devShells.x86_64-linux.default = pkgs.mkShell {
      nativeBuildInputs = with pkgs; [rustPlatform.bindgenHook];
      buildInputs = with pkgs; [cmake pkg-config libz libclang llvm];
    };
  };
}
