<template>
  <Dialog :open="open" @update:open="$emit('update:open', $event)">
    <DialogContent class="sm:max-w-md">
      <DialogHeader>
        <DialogTitle>{{ $t('sync.login-title') }}</DialogTitle>
      </DialogHeader>

      <div class="login-step-wrapper relative overflow-hidden">
        <Transition :name="transitionName" mode="out-in">
          <div v-if="step === 'email'" key="email" class="flex flex-col gap-4">
        <div v-if="emailEnabled" class="flex flex-col gap-2">
          <label class="text-sm font-medium">{{ $t('sync.login-email') }}</label>
          <Input
            v-model="email"
            type="email"
            :placeholder="$t('sync.login-email-placeholder')"
            @keyup.enter="sendCode"
          />
        </div>
        <p v-if="error" class="text-sm text-destructive">{{ error }}</p>
        <Button v-if="emailEnabled" :disabled="loading" @click="sendCode">
          {{ loading ? '...' : $t('sync.login-send-code') }}
        </Button>

        <div v-if="emailEnabled && githubEnabled" class="relative my-2">
          <div class="absolute inset-0 flex items-center">
            <span class="w-full border-t" />
          </div>
          <div class="relative flex justify-center text-xs">
            <span class="bg-background px-2 text-muted-foreground">{{ $t('sync.login-or') }}</span>
          </div>
        </div>

        <Button v-if="githubEnabled" variant="outline" :disabled="loading" @click="loginGitHub">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M12 .297c-6.63 0-12 5.373-12 12 0 5.303 3.438 9.8 8.205 11.385.6.113.82-.258.82-.577 0-.285-.01-1.04-.015-2.04-3.338.724-4.042-1.61-4.042-1.61C4.422 18.07 3.633 17.7 3.633 17.7c-1.087-.744.084-.729.084-.729 1.205.084 1.838 1.236 1.838 1.236 1.07 1.835 2.809 1.305 3.495.998.108-.776.417-1.305.76-1.605-2.665-.3-5.466-1.332-5.466-5.93 0-1.31.465-2.38 1.235-3.22-.135-.303-.54-1.523.105-3.176 0 0 1.005-.322 3.3 1.23.96-.267 1.98-.399 3-.405 1.02.006 2.04.138 3 .405 2.28-1.552 3.285-1.23 3.285-1.23.645 1.653.24 2.873.12 3.176.765.84 1.23 1.91 1.23 3.22 0 4.61-2.805 5.625-5.475 5.92.42.36.81 1.096.81 2.22 0 1.606-.015 2.896-.015 3.286 0 .315.21.69.825.57C20.565 22.092 24 17.592 24 12.297c0-6.627-5.373-12-12-12"/></svg>
          {{ $t('sync.login-github') }}
        </Button>

        <p v-if="!emailEnabled && !githubEnabled" class="text-sm text-muted-foreground">
          {{ $t('sync.login-no-methods') }}
        </p>
      </div>

      <div v-else-if="step === 'otp'" class="flex flex-col gap-4">
        <p class="text-sm text-muted-foreground">{{ $t(linkOnly ? 'sync.login-link-only' : 'sync.login-code-sent') }}</p>
        <div class="flex flex-col gap-2">
          <label class="text-sm font-medium">{{ $t('sync.login-verify-code') }}</label>
          <Input
            v-model="code"
            :placeholder="$t('sync.login-verify-code-placeholder')"
            maxlength="6"
            class="text-center text-lg tracking-widest"
            @keyup.enter="verifyCode"
          />
        </div>
        <p v-if="error" class="text-sm text-destructive">{{ error }}</p>
        <div class="flex gap-2">
          <Button variant="outline" @click="goBack">
            {{ $t('sync.login-back') }}
          </Button>
          <Button :disabled="loading" @click="verifyCode">
            {{ loading ? '...' : $t('sync.login-verify') }}
          </Button>
        </div>
      </div>
        </Transition>
      </div>
    </DialogContent>
  </Dialog>
</template>

<script lang="ts">
import { computed, defineComponent, ref, watch } from "vue";
import { getLocaleManager } from "@/components/Locale";
import { Button } from "@/components/ui/button";
import {
	Dialog,
	DialogContent,
	DialogHeader,
	DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { useAuthStore } from "@/store/auth";

export default defineComponent({
	name: "LoginModal",
	components: {
		Button,
		Dialog,
		DialogContent,
		DialogHeader,
		DialogTitle,
		Input,
	},
	props: {
		open: { type: Boolean, default: false },
	},
	emits: ["update:open", "success"],
	setup(_props, { emit }) {
		const authStore = useAuthStore();
		const t = getLocaleManager().getI18n().t;
		const email = ref("");
		const code = ref("");
		const step = ref<"email" | "otp">("email");
		const transitionName = ref("slide-left");
		const loading = ref(false);
		const linkOnly = ref(false);
		const error = ref("");

		const sendCode = async () => {
			error.value = "";
			if (!email.value || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.value)) {
				error.value = t("sync.login-invalid-email");
				return;
			}
			loading.value = true;
			try {
				const sent = await authStore.requestOTP(email.value);
				linkOnly.value = sent.linkOnly;
				transitionName.value = "slide-left";
				step.value = "otp";
			} catch (err) {
				error.value = (err as Error).message || t("sync.login-otp-fail");
			} finally {
				loading.value = false;
			}
		};

		const goBack = () => {
			transitionName.value = "slide-right";
			step.value = "email";
		};

		const verifyCode = async () => {
			error.value = "";
			if (code.value?.length !== 6) {
				error.value = t("sync.login-invalid-code");
				return;
			}
			loading.value = true;
			try {
				await authStore.verifyOTP(email.value, code.value);
			} catch (err) {
				error.value = (err as Error).message || t("sync.login-verify-fail");
			} finally {
				loading.value = false;
			}
		};

		const loginGitHub = async () => {
			error.value = "";
			loading.value = true;
			try {
				await authStore.loginWithGitHub();
			} catch (err) {
				error.value = (err as Error).message || t("sync.login-github-fail");
			} finally {
				loading.value = false;
			}
		};

		const reset = () => {
			email.value = "";
			code.value = "";
			step.value = "email";
			transitionName.value = "slide-left";
			linkOnly.value = false;
			error.value = "";
		};

		watch(
			() => authStore.isLoggedIn,
			(isLoggedIn) => {
				if (isLoggedIn) {
					emit("success");
					emit("update:open", false);
					reset();
				}
			},
		);

		watch(
			() => _props.open,
			(isOpen) => {
				if (!isOpen) {
					reset();
				}
			},
		);

		const emailEnabled = computed(
			() => authStore.serverConfig?.email !== false,
		);
		const githubEnabled = computed(
			() =>
				authStore.serverConfig?.github === true &&
				authStore.serverConfig.loginHandoff,
		);

		return {
			email,
			code,
			step,
			transitionName,
			loading,
			linkOnly,
			error,
			emailEnabled,
			githubEnabled,
			sendCode,
			verifyCode,
			goBack,
			loginGitHub,
		};
	},
});
</script>

<style scoped>
.login-step-wrapper {
	min-height: 220px;
}

.slide-left-enter-active,
.slide-left-leave-active,
.slide-right-enter-active,
.slide-right-leave-active {
	transition: transform 0.25s ease, opacity 0.25s ease;
	position: absolute;
	width: 100%;
	left: 0;
	will-change: transform;
}

.slide-left-enter-from {
	transform: translateX(100%);
	opacity: 0;
}

.slide-left-enter-to {
	transform: translateX(0);
	opacity: 1;
}

.slide-left-leave-from {
	transform: translateX(0);
	opacity: 1;
}

.slide-left-leave-to {
	transform: translateX(-100%);
	opacity: 0;
}

.slide-right-enter-from {
	transform: translateX(-100%);
	opacity: 0;
}

.slide-right-enter-to {
	transform: translateX(0);
	opacity: 1;
}

.slide-right-leave-from {
	transform: translateX(0);
	opacity: 1;
}

.slide-right-leave-to {
	transform: translateX(100%);
	opacity: 0;
}
</style>
