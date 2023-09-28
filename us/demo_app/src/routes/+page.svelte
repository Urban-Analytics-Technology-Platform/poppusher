<script lang="ts">
	import MetricExplorer from '../components/MetricExplorer/MetricExplorer.svelte';
	import MetricTable from '../components/MetricTable/MetricTable.svelte';
	import Map from '../components/Map/Map.svelte';
</script>

<div class="main_area">
	<MetricExplorer />
	<div class="viz">
		<Map />
		<MetricTable />
	</div>
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
	.viz {
		flex: 1;
		display: flex;
		flex-direction: column;
	}
</style>
