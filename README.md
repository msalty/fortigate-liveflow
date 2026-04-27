# FortiGate LiveFlow

Lightweight standalone live traffic dashboard for FortiGate sessions in Go and HTML.

## Run

```bash
go run .
```

Then open http://localhost:8080.

## Recent feature additions

- Conversation table includes optional source device and destination FQDN columns.
- Internal device resolution can be enabled in **Resolution settings**.
  - It attempts to run the configured FortiGate command, defaulting to `diagnose user device list`, through the configured HTTPS API path.
  - The default command API path is `/api/v2/monitor/user/device/query`. If your FortiOS build exposes CLI execution through a different monitor endpoint, change it in the UI.
- External server reverse DNS can be enabled with a user-specified DNS server.
- Settings can be saved to the local user config directory.
  - By default, credentials are not saved unless **Also save token/password** is enabled.

## Security note

Use a dedicated read-only API admin/token where possible. Saving tokens/passwords writes them to a local JSON settings file with user-only file permissions where the OS supports it.

## v0.15 changes

- Added an egress-interface filter above the conversations table.
- Selecting an interface immediately filters the main conversation chart, aggregate overview chart, and conversations table.
- Clear Interface Filter restores the all-flow view.
- Show All / Hide All now applies to the current filtered table view.
