<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD041 -->

<script defer src="https://pyscript.net/latest/pyscript.js"></script>

<py-config>
  packages = ["repid"]
</py-config>

???+ info
    This is a playground where you can edit and run code in your browser using PyScript -
    give it a try, it's awesome!

<div id="input" contentEditable="true">

```title="Edit me!"
async def main() -> None:
    import repid

    app = repid.Repid(repid.Connection(repid.InMemoryMessageBroker()))

    router = repid.Router()

    @router.actor
    async def string_length(the_string: str) -> int:
        print(the_string)
        await asyncio.sleep(1)
        print("I've slept well...")
        return len(the_string)

    async with app.magic():
        hello_job = repid.Job(
            "string_length",
            args=dict(the_string="Hello world!"),
        )
        await hello_job.queue.declare()
        await hello_job.enqueue()
        worker = repid.Worker(routers=[router], messages_limit=1, handle_signals=[])
        await worker.run()
```

</div>

<py-script>
    import asyncio
    from pyscript import Element
    from html.parser import HTMLParser
    def eval_python():
        class HTMLFilter(HTMLParser):
            text = ""
            def handle_data(self, data):
                self.text += data
        f = HTMLFilter()
        input_html = Element("input").innerHtml
        input_html = input_html.replace("<br>", "\n")  # user inputted new lines == "<br>"
        f.feed(input_html)
        f.text = f.text[10:]  # remove "Edit me!"
        locals = {}
        exec(f.text, globals(), locals)
        asyncio.create_task(locals.get("main")())
</py-script>

<button py-click="eval_python()" class="py-button">Run main()!</button>

---

Your output will be here:

<py-terminal></py-terminal>
