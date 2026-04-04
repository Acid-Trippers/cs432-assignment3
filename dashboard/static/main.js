async function refreshExternalApiWarning() {
	const warning = document.getElementById("external-api-warning");
	if (!warning) return;

	try {
		const response = await fetch("/api/stats", { method: "GET" });
		if (!response.ok) {
			warning.classList.remove("hidden");
			return;
		}

		const data = await response.json();
		const reachable = Boolean(data.external_api_reachable);
		warning.classList.toggle("hidden", reachable);
	} catch {
		warning.classList.remove("hidden");
	}
}

refreshExternalApiWarning();
setInterval(refreshExternalApiWarning, 10000);
