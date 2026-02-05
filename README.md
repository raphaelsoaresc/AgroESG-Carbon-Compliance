# Comandos Essenciais do Projeto (WIP)

Este é um guia rápido de referência para gerenciar o ambiente e as dependências deste projeto.

## Gerenciamento de Ambiente (devenv / Nix)

| Comando | Função Técnica |
| :--- | :--- |
| `devenv up` | Inicia os serviços definidos no devenv.nix (como o PostgreSQL) e sincroniza o ambiente. Use sempre que iniciar o trabalho. |
| `devenv shell` | Entra no ambiente virtual do projeto, carregando todas as dependências e variáveis de ambiente (onde uv e python funcionam). |
| `exit` | Sai do devenv shell (comando padrão do terminal Linux/Debian). |

## Gerenciamento de Dependências Python (uv)

| Comando | Função Técnica |
| :--- | :--- |
| `uv sync` | Sincroniza as bibliotecas instaladas no seu .venv com as listadas no pyproject.toml (instala/atualiza as dependências). |
| `uv pip show <pacote>` | Verifica se um pacote específico (ex: geoalchemy2) está instalado e qual a versão dele. |
| `uv tree` | Mostra todas as dependências do projeto em formato de árvore, útil para debug de conflitos. |
