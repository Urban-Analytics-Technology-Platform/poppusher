<script lang="ts">
	import MetricExplorer from '../components/MetricExplorer/MetricExplorer.svelte';
	import MetricTable from '../components/MetricTable/MetricTable.svelte';
</script>

<div class="main_area">
	<MetricExplorer />
	<MetricTable />
</div>

<style>
	.main_area {
		width: 100%;
		height: 100%;
		padding: 20px;
		box-sizing: border-box;
		display: flex;
		flex-direction: row;
	}
</style>
