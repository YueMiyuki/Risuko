import { createRouter, createWebHashHistory } from "vue-router";
import Main from "@/components/Main.vue";
import TaskIndex from "@/components/Task/Index.vue";

export default createRouter({
	history: createWebHashHistory(),
	routes: [
		{
			path: "/",
			name: "main",
			component: Main,
			children: [
				{
					path: "/task",
					alias: "/",
					component: TaskIndex,
					props: {
						status: "all",
					},
				},
				{
					path: "/task/:status",
					name: "task",
					component: TaskIndex,
					props: true,
				},
				{
					path: "/share",
					name: "share",
					component: () => import("@/components/Share/Index.vue"),
				},
				{
					path: "/health",
					name: "health",
					component: () => import("@/components/Health/Index.vue"),
				},
				{
					path: "/stats",
					name: "stats",
					component: () => import("@/components/Stats/StatsPage.vue"),
				},
				{
					path: "/preference",
					name: "preference",
					component: () => import("@/components/Preference/Index.vue"),
					redirect: "/preference/basic",
					props: true,
					children: [
						{
							path: "basic",
							alias: "",
							component: () => import("@/components/Preference/Basic.vue"),
						},
						{
							path: "appearance",
							component: () => import("@/components/Preference/Appearance.vue"),
						},
						{
							path: "advanced",
							component: () => import("@/components/Preference/Advanced.vue"),
						},
						{
							path: "usenet",
							component: () => import("@/components/Preference/Usenet.vue"),
						},
						{
							path: "cloud-sinks",
							component: () => import("@/components/Preference/CloudSinks.vue"),
						},
						{
							path: "sync",
							component: () => import("@/components/Preference/Sync.vue"),
						},
					],
				},
			],
		},
		{
			path: "/:pathMatch(.*)*",
			redirect: "/",
		},
	],
});
