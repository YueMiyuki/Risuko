import risuko from "../packages/risuko-js/index.js";

(async () => {
	await risuko.startEngine();
	risuko.onEvent((event, gid) => {
		console.log(gid, event);
	});
	const gid = await risuko.addUri([
		"https://cdn.hotelnearmedanta.com/testfile.org/testfile.org-5GB.dat",
	]);
	console.log("Started download, GID:", gid);
})();
