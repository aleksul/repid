# Contributing to repid

Your contributions are invaluable to our community - thank you for being a part of the journey!

## Development setup

### Quick overview

1. [Create a fork](https://github.com/aleksul/repid/fork) of repid
2. Clone your fork
3. Install [uv](https://docs.astral.sh/uv/getting-started/installation/)
4. Install dependencies in virtual environment
5. Configure pre-commit
6. Make your changes!

### Detailed guide

#### Create a fork and clone the repo

First of all, you will need to [create a fork](https://github.com/aleksul/repid/fork).

Clone it and `cd` into the directory:

```bash
git clone https://github.com/__your_username__/repid.git
cd repid
```

#### uv & venv

Repid uses [uv](https://docs.astral.sh/uv) to manage virtual environment, dependencies &
package the project.

After installation is done, run the following command in the project's root directory:

```bash
make install
```

It will create virtual environment and install inside of it all dependencies, including those,
which are needed for development.

!!! important
    Use lowest supported by `repid` version of Python (== 3.10 for now) for your venv.

To activate a venv run:

```bash
source .venv/bin/activate
```

...or simply execute the needed command with `uv run` prefix, e.g.

```bash
uv run mkdocs serve
```

#### pre-commit

`repid` uses [pre-commit](https://pre-commit.com) to run linters and formatters.

If you want to run all linters and formatters, execute the following command:

```bash
uv run pre-commit run -a
```

If you want to commit some changes disregarding pre-commit hooks, add `-n` or `--no-verify` flag
to `git commit` command.

!!! important
    Keep in mind that when you will submit your pull request, all the hooks
    must pass in CI anyway, or, unfortunately, we will have to decline your contribution.

#### Make your changes

Make your changes, create commits and submit a pull request.

Here are some advices:

1. Commits:
    - Please use [gitmoji](https://gitmoji.dev) to prefix your commit messages
    - Try to make commits atomic
    - gitmoji should describe type of the change, while commit message
    shows the exact change in behavior, e.g.

    ```shell
    ♻️ Refactored _signal_emitter property and wrapping in new into a separate _WrapperABC class
    ```

2. Issues:
    - Please use one of provided templates to [create an issue](https://github.com/aleksul/repid/issues/new/choose)
    - Make it as much descriptive as possible

3. Pull requests:
    - If your pull request isn't very simple (like fixing a typo in the docs) - please create an
    issue first, so we can discuss it
    - Mark pull request as a resolver for the related issue(-s)
    - Please complete the checklist provided in the pull request template
    - Please avoid creating very large pull requests

#### Running tests locally

You will need [docker](https://www.docker.com) to run tests locally.

Apart from that, everything is automated via pytest (including creation of containers
for integration testing!), so all you need to do is:

```bash
uv run pytest
```

If you want to run test suite with all default arguments already set up - there is a prepared script:

```bash
make test
```

#### VSCode and Dev Container

If you are willing to use VSCode, repid comes with some configs already in place. Feel free to
modify them up to your liking, but be careful not to commit any changes unintentionally.

Another quality-of-life feature is [Dev Container](https://containers.dev). Essentially it is
a bunch of configs, which describe how to create a docker container, that comes with VSCode server,
all the necessary plugins and a virtual environment already set up.
You can use it with [GitHub Codespaces](https://docs.github.com/codespaces/overview) to
[create it in one click](https://github.com/codespaces/new?ref=main&repo=420659467)
and start developing right away!
