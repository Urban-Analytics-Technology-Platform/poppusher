
// this file is generated — do not edit it


/// <reference types="@sveltejs/kit" />

/**
 * Environment variables [loaded by Vite](https://vitejs.dev/guide/env-and-mode.html#env-files) from `.env` files and `process.env`. Like [`$env/dynamic/private`](https://kit.svelte.dev/docs/modules#$env-dynamic-private), this module cannot be imported into client-side code. This module only includes variables that _do not_ begin with [`config.kit.env.publicPrefix`](https://kit.svelte.dev/docs/configuration#env) _and do_ start with [`config.kit.env.privatePrefix`](https://kit.svelte.dev/docs/configuration#env) (if configured).
 * 
 * _Unlike_ [`$env/dynamic/private`](https://kit.svelte.dev/docs/modules#$env-dynamic-private), the values exported from this module are statically injected into your bundle at build time, enabling optimisations like dead code elimination.
 * 
 * ```ts
 * import { API_KEY } from '$env/static/private';
 * ```
 * 
 * Note that all environment variables referenced in your code should be declared (for example in an `.env` file), even if they don't have a value until the app is deployed:
 * 
 * ```
 * MY_FEATURE_FLAG=""
 * ```
 * 
 * You can override `.env` values from the command line like so:
 * 
 * ```bash
 * MY_FEATURE_FLAG="enabled" npm run dev
 * ```
 */
declare module '$env/static/private' {
	export const NVM_RC_VERSION: string;
	export const npm_package_dependencies_duckdb: string;
	export const npm_package_devDependencies_prettier: string;
	export const ZELLIJ: string;
	export const npm_package_devDependencies_eslint_plugin_svelte: string;
	export const npm_package_dependencies_lit_html: string;
	export const NODE: string;
	export const NVM_CD_FLAGS: string;
	export const npm_package_devDependencies_prettier_plugin_svelte: string;
	export const npm_package_devDependencies_typescript: string;
	export const INIT_CWD: string;
	export const SHELL: string;
	export const TERM: string;
	export const npm_package_devDependencies_vite: string;
	export const npm_package_dependencies__spectrum_web_components_theme: string;
	export const TMPDIR: string;
	export const npm_package_dependencies__spectrum_web_components_bundle: string;
	export const npm_package_scripts_lint: string;
	export const WINDOWID: string;
	export const npm_package_scripts_dev: string;
	export const npm_package_private: string;
	export const npm_package_devDependencies__sveltejs_kit: string;
	export const npm_config_registry: string;
	export const LC_ALL: string;
	export const ZSH: string;
	export const PNPM_HOME: string;
	export const USER: string;
	export const NVM_DIR: string;
	export const LS_COLORS: string;
	export const npm_package_scripts_check_watch: string;
	export const ALACRITTY_SOCKET: string;
	export const COMMAND_MODE: string;
	export const PNPM_SCRIPT_SRC_DIR: string;
	export const npm_package_dependencies_svelte_virtual_table: string;
	export const ALACRITTY_LOG: string;
	export const SSH_AUTH_SOCK: string;
	export const npm_package_dependencies_lodash: string;
	export const __CF_USER_TEXT_ENCODING: string;
	export const npm_package_devDependencies_eslint: string;
	export const npm_package_devDependencies__typescript_eslint_eslint_plugin: string;
	export const npm_package_devDependencies_tslib: string;
	export const npm_execpath: string;
	export const PAGER: string;
	export const npm_package_devDependencies_svelte: string;
	export const LSCOLORS: string;
	export const npm_config_frozen_lockfile: string;
	export const npm_package_devDependencies__typescript_eslint_parser: string;
	export const npm_package_dependencies_just_flush: string;
	export const PATH: string;
	export const npm_package_dependencies__spectrum_web_components_iconset: string;
	export const npm_config_engine_strict: string;
	export const __CFBundleIdentifier: string;
	export const NVM_NODEJS_ORG_MIRROR: string;
	export const PWD: string;
	export const npm_command: string;
	export const npm_package_scripts_preview: string;
	export const npm_package_devDependencies__types_lodash: string;
	export const npm_lifecycle_event: string;
	export const npm_package_name: string;
	export const npm_package_scripts_test_integration: string;
	export const npm_config_resolution_mode: string;
	export const NODE_PATH: string;
	export const npm_package_scripts_build: string;
	export const XPC_FLAGS: string;
	export const npm_package_devDependencies_vitest: string;
	export const npm_package_devDependencies_eslint_config_prettier: string;
	export const npm_config_node_gyp: string;
	export const XPC_SERVICE_NAME: string;
	export const npm_package_version: string;
	export const npm_package_devDependencies__sveltejs_adapter_auto: string;
	export const npm_package_devDependencies_svelte_check: string;
	export const HOME: string;
	export const SHLVL: string;
	export const npm_package_type: string;
	export const npm_package_scripts_test: string;
	export const npm_package_dependencies_svelte_maplibre: string;
	export const npm_package_dependencies__sveltejs_svelte_virtual_list: string;
	export const LOGNAME: string;
	export const LESS: string;
	export const npm_package_scripts_format: string;
	export const ALACRITTY_WINDOW_ID: string;
	export const npm_lifecycle_script: string;
	export const npm_package_dependencies_pmtiles: string;
	export const npm_config_user_agent: string;
	export const npm_package_devDependencies__playwright_test: string;
	export const npm_package_dependencies_maplibre_gl: string;
	export const npm_package_scripts_check: string;
	export const COLORTERM: string;
	export const ZELLIJ_SESSION_NAME: string;
	export const npm_package_scripts_test_unit: string;
	export const npm_node_execpath: string;
	export const NODE_ENV: string;
}

/**
 * Similar to [`$env/static/private`](https://kit.svelte.dev/docs/modules#$env-static-private), except that it only includes environment variables that begin with [`config.kit.env.publicPrefix`](https://kit.svelte.dev/docs/configuration#env) (which defaults to `PUBLIC_`), and can therefore safely be exposed to client-side code.
 * 
 * Values are replaced statically at build time.
 * 
 * ```ts
 * import { PUBLIC_BASE_URL } from '$env/static/public';
 * ```
 */
declare module '$env/static/public' {
	
}

/**
 * This module provides access to runtime environment variables, as defined by the platform you're running on. For example if you're using [`adapter-node`](https://github.com/sveltejs/kit/tree/master/packages/adapter-node) (or running [`vite preview`](https://kit.svelte.dev/docs/cli)), this is equivalent to `process.env`. This module only includes variables that _do not_ begin with [`config.kit.env.publicPrefix`](https://kit.svelte.dev/docs/configuration#env) _and do_ start with [`config.kit.env.privatePrefix`](https://kit.svelte.dev/docs/configuration#env) (if configured).
 * 
 * This module cannot be imported into client-side code.
 * 
 * ```ts
 * import { env } from '$env/dynamic/private';
 * console.log(env.DEPLOYMENT_SPECIFIC_VARIABLE);
 * ```
 * 
 * > In `dev`, `$env/dynamic` always includes environment variables from `.env`. In `prod`, this behavior will depend on your adapter.
 */
declare module '$env/dynamic/private' {
	export const env: {
		NVM_RC_VERSION: string;
		npm_package_dependencies_duckdb: string;
		npm_package_devDependencies_prettier: string;
		ZELLIJ: string;
		npm_package_devDependencies_eslint_plugin_svelte: string;
		npm_package_dependencies_lit_html: string;
		NODE: string;
		NVM_CD_FLAGS: string;
		npm_package_devDependencies_prettier_plugin_svelte: string;
		npm_package_devDependencies_typescript: string;
		INIT_CWD: string;
		SHELL: string;
		TERM: string;
		npm_package_devDependencies_vite: string;
		npm_package_dependencies__spectrum_web_components_theme: string;
		TMPDIR: string;
		npm_package_dependencies__spectrum_web_components_bundle: string;
		npm_package_scripts_lint: string;
		WINDOWID: string;
		npm_package_scripts_dev: string;
		npm_package_private: string;
		npm_package_devDependencies__sveltejs_kit: string;
		npm_config_registry: string;
		LC_ALL: string;
		ZSH: string;
		PNPM_HOME: string;
		USER: string;
		NVM_DIR: string;
		LS_COLORS: string;
		npm_package_scripts_check_watch: string;
		ALACRITTY_SOCKET: string;
		COMMAND_MODE: string;
		PNPM_SCRIPT_SRC_DIR: string;
		npm_package_dependencies_svelte_virtual_table: string;
		ALACRITTY_LOG: string;
		SSH_AUTH_SOCK: string;
		npm_package_dependencies_lodash: string;
		__CF_USER_TEXT_ENCODING: string;
		npm_package_devDependencies_eslint: string;
		npm_package_devDependencies__typescript_eslint_eslint_plugin: string;
		npm_package_devDependencies_tslib: string;
		npm_execpath: string;
		PAGER: string;
		npm_package_devDependencies_svelte: string;
		LSCOLORS: string;
		npm_config_frozen_lockfile: string;
		npm_package_devDependencies__typescript_eslint_parser: string;
		npm_package_dependencies_just_flush: string;
		PATH: string;
		npm_package_dependencies__spectrum_web_components_iconset: string;
		npm_config_engine_strict: string;
		__CFBundleIdentifier: string;
		NVM_NODEJS_ORG_MIRROR: string;
		PWD: string;
		npm_command: string;
		npm_package_scripts_preview: string;
		npm_package_devDependencies__types_lodash: string;
		npm_lifecycle_event: string;
		npm_package_name: string;
		npm_package_scripts_test_integration: string;
		npm_config_resolution_mode: string;
		NODE_PATH: string;
		npm_package_scripts_build: string;
		XPC_FLAGS: string;
		npm_package_devDependencies_vitest: string;
		npm_package_devDependencies_eslint_config_prettier: string;
		npm_config_node_gyp: string;
		XPC_SERVICE_NAME: string;
		npm_package_version: string;
		npm_package_devDependencies__sveltejs_adapter_auto: string;
		npm_package_devDependencies_svelte_check: string;
		HOME: string;
		SHLVL: string;
		npm_package_type: string;
		npm_package_scripts_test: string;
		npm_package_dependencies_svelte_maplibre: string;
		npm_package_dependencies__sveltejs_svelte_virtual_list: string;
		LOGNAME: string;
		LESS: string;
		npm_package_scripts_format: string;
		ALACRITTY_WINDOW_ID: string;
		npm_lifecycle_script: string;
		npm_package_dependencies_pmtiles: string;
		npm_config_user_agent: string;
		npm_package_devDependencies__playwright_test: string;
		npm_package_dependencies_maplibre_gl: string;
		npm_package_scripts_check: string;
		COLORTERM: string;
		ZELLIJ_SESSION_NAME: string;
		npm_package_scripts_test_unit: string;
		npm_node_execpath: string;
		NODE_ENV: string;
		[key: `PUBLIC_${string}`]: undefined;
		[key: `${string}`]: string | undefined;
	}
}

/**
 * Similar to [`$env/dynamic/private`](https://kit.svelte.dev/docs/modules#$env-dynamic-private), but only includes variables that begin with [`config.kit.env.publicPrefix`](https://kit.svelte.dev/docs/configuration#env) (which defaults to `PUBLIC_`), and can therefore safely be exposed to client-side code.
 * 
 * Note that public dynamic environment variables must all be sent from the server to the client, causing larger network requests — when possible, use `$env/static/public` instead.
 * 
 * ```ts
 * import { env } from '$env/dynamic/public';
 * console.log(env.PUBLIC_DEPLOYMENT_SPECIFIC_VARIABLE);
 * ```
 */
declare module '$env/dynamic/public' {
	export const env: {
		[key: `PUBLIC_${string}`]: string | undefined;
	}
}
