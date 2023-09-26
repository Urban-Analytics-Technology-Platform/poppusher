<script>
	import '@spectrum-web-components/theme/scale-medium.js';
	import '@spectrum-web-components/theme/sp-theme.js';
	import '@spectrum-web-components/theme/theme-lightest.js';
	import '@spectrum-web-components/bundle';
	import '@spectrum-web-components/bundle/elements.js';
</script>

<sp-theme theme="spectrum" color="lightest" scale="medium">
	<div class="layout-grid">
		<div class="nav">
			<sp-top-nav>
				<sp-top-nav-item href="/">ACS Explorer</sp-top-nav-item>
				<sp-top-nav-item href="/about" style="margin-inline-start: auto;"> About </sp-top-nav-item>
				<sp-top-nav-item href="/info">Info</sp-top-nav-item>
				<sp-action-menu
					label="Account"
					placement="bottom-end"
					style="margin-inline-start: auto;"
					quiet
				>
					<sp-menu-item>Account Settings</sp-menu-item>
					<sp-menu-item>My Profile</sp-menu-item>
					<sp-menu-divider />
					<sp-menu-item>Share</sp-menu-item>
					<sp-menu-divider />
					<sp-menu-item>Help</sp-menu-item>
					<sp-menu-item>Sign Out</sp-menu-item>
				</sp-action-menu>
			</sp-top-nav>
		</div>

		<div class="main">
			<slot />
		</div>
	</div>
</sp-theme>

<style>
	:global(body) {
		width: 100vw;
		height: 100vh;
		margin: 0px;
		padding: 0px;
	}
	.nav {
		grid-area: nav;
	}
	.main {
		grid-area: main;
	}
	.layout-grid {
		display: grid;
		grid-template-rows: 50px 1fr;
		grid-template-columns: 200px 1fr;
		grid-template-areas:
			'nav nav'
			'main main';
	}
</style>
