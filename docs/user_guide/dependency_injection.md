# Dependency injection

## Depends

In your actor, you can declare a dependency as follows:

=== "Python 3.10+"

    ```python hl_lines="12"
    from typing import Annotated

    from repid import Depends


    def dependency_function() -> str:
        return "Hello!"


    @router.actor  # (1)
    async def my_actor(
        my_dependency: Annotated[str, Depends(dependency_function)]
    ) -> None:
        print(my_dependency)  # (2)
    ```

    1. Router declaration is omitted
    2. Will print `Hello!`

=== "Python 3.8+"

    ```python  hl_lines="11"
    from typing_extensions import Annotated  # (1)
    from repid import Depends


    def dependency_function() -> str:
        return "Hello!"


    @router.actor  # (2)
    async def my_actor(
        my_dependency: Annotated[str, Depends(dependency_function)]
    ) -> None:
        print(my_dependency)  # (3)
    ```

    1. Annotated was added in Python 3.10, but you can use backported version from
    `typing_extenstions` in earlier versions
    2. Router declaration is omitted
    3. Will print `Hello!`

## Sub-dependencies

Your dependency can also have some dependencies of its own!

=== "Python 3.10+"

    ```python hl_lines="11"
    from typing import Annotated

    from repid import Depends


    def subdependency_function() -> str:
        return "world!"


    def dependency_function(
        sub: Annotated[str, Depends(subdependency_function)]
    ) -> str:
        return "Hello " + sub


    @router.actor  # (1)
    async def my_actor(
        my_dependency: Annotated[str, Depends(dependency_function)]
    ) -> None:
        print(my_dependency)  # (2)
    ```

    1. Router declaration is omitted
    2. Will print `Hello world!`

=== "Python 3.8+"

    ```python  hl_lines="10"
    from typing_extensions import Annotated  # (1)
    from repid import Depends


    def subdependency_function() -> str:
        return "world!"


    def dependency_function(
        sub: Annotated[str, Depends(subdependency_function)]
    ) -> str:
        return "Hello " + sub


    @router.actor  # (2)
    async def my_actor(
        my_dependency: Annotated[str, Depends(dependency_function)]
    ) -> None:
        print(my_dependency)  # (3)
    ```

    1. Annotated was added in Python 3.10, but you can use backported version from
    `typing_extenstions` in earlier versions
    2. Router declaration is omitted
    3. Will print `Hello world!`

## Sync and Async dependencies

Your dependencies' functions can be both synchronous and asynchronous.

### Asynchronous

```python hl_lines="1"
async def dependency_function() -> str:
    await asyncio.sleep(0.1)  # (1)
    return "Hello world!"

Depends(dependency_function)
```

1. Imitates some async work

### Synchronous

```python hl_lines="1"
def dependency_function() -> str:
    return "Hello world!"

Depends(dependency_function)
```

### Synchronous (CPU-heavy)

In case your function is synchronous, it will be run in a thread pool executor to avoid blocking
the event loop.

You can also opt to run it in a process pool executor, if, for example, your function is CPU bound.

```python hl_lines="5"
def dependency_function() -> int:
    # some CPU-heavy computation here
    return 123

Depends(dependency_function, run_in_process=True)
```

## Overriding dependencies

You can override a dependency (e.g. for test purposes).

!!! Warning "Beware global state mutation"
    Overrides can be extremly helpful in tests, especially when you want to do complete
    end-to-end testing.

    However, as overrides are essentially a global state mutations, you have to be very careful if
    they are used inside of the application itself.

```python hl_lines="6"
def dependency_function() -> str:
    return "Hello!"

d = Depends(dependency_function)

d.override(lambda: "Overriden!")  # (1)

@router.actor
async def my_actor(
    my_dependency: Annotated[str, d]
) -> None:
    print(my_dependency)  # (2)
```

1. You can specify any other function here and even select `run_in_process` if you need
2. Will print `Overriden!`
