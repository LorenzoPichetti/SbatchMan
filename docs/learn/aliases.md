# SbatchMan Command Aliases

The `sbatchman` CLI provides powerful functionality, but the commands can be lengthy to type repeatedly. To improve your workflow, you can add convenient aliases to your shell configuration (e.g., `.bashrc` or `.bash_profile` or `.zshrc`). This page provides a ready-to-use Bash script for setting up useful aliases for common `sbatchman` commands.

## Recommended Aliases

| Command                         | Alias   |
|---------------------------------|---------|
| `sbatchman`                     | `sbm`  |
| `sbatchman init`                | `sbmi`  |
| `sbatchman launch`              | `sbml`  |
| `sbatchman status`              | `sbms`  |
| `sbatchman archive`             | `sbma`  |
| `sbatchman delete-jobs`         | `sbmdj` |
| `sbatchman configure`           | `sbmc`  |

Append `h` to any alias to get the help message for the command.

You can customize or add more aliases as needed.

## Bash Script to Add Aliases

Copy and paste the following script into your terminal to automatically append these aliases to your shell configuration file file:

```bash
if [ -n "$ZSH_VERSION" ]; then
    SHELL_RC="$HOME/.zshrc"
elif [ -n "$BASH_VERSION" ]; then
    SHELL_RC="$HOME/.bashrc"
else
    SHELL_RC="$HOME/.profile"
fi
cat << 'EOF' >> "$SHELL_RC"
# SbatchMan CLI Aliases
alias sbm='sbatchman'
alias sbmh='sbatchman --help'
alias sbmi='sbatchman init'
alias sbmih='sbatchman init --help'
alias sbml='sbatchman launch'
alias sbmlh='sbatchman launch --help'
alias sbms='sbatchman status'
alias sbmsh='sbatchman status --help'
alias sbma='sbatchman archive'
alias sbmah='sbatchman archive --help'
alias sbmdj='sbatchman delete-jobs'
alias sbmdjh='sbatchman delete-jobs --help'
alias sbmc='sbatchman configure'
alias sbmch='sbatchman configure --help'
alias sbmh='sbatchman --help'
# End SbatchMan Aliases
EOF
echo "SbatchMan aliases added to $SHELL_RC. Run 'source $SHELL_RC' to activate them."
```