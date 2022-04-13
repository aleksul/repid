# TODO: check `repid.middlewares.middleware.Events` literal against `repid.connections.connection Messaging and Resulting`

for name in dir(Messaging) + dir(Resulting):
    if name.startswith("__"):
        continue
    if callable(getattr(Messaging, name)) or callable(getattr(Resulting, name)):
        events.append(f"before_{name}")
        events.append(f"after_{name}")
