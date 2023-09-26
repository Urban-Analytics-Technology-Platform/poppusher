<script lang="ts">
	import Layout from '../../routes/+layout.svelte';
	import { selectedMetrics, metricValues } from '../../stores/metrics';
	import { html } from 'lit-html';
	$: metricValues.setIds($selectedMetrics);

	$: console.log('metric values ', $metricValues);
</script>

<div class="container">
	{#if $selectedMetrics.length === 0}
		<p>Select some metrics to load</p>
	{:else if $metricValues.loading}
		<p>Loading</p>
	{:else if $metricValues.value.error}
		<p>Something went wrong {$metricValues.value.error}</p>
	{:else}
		<sp-table>
			<sp-table-head>
				<sp-table-head-cell>GeoID</sp-table-head-cell>
				{#each $selectedMetrics as metricId}
					<sp-table-head-cell>{metricId}</sp-table-head-cell>
				{/each}
			</sp-table-head>
			<sp-table-body>
				{#each $metricValues.value.slice(0, 20) as row}
					<sp-table-row>
						{#each Object.values(row) as val}
							<sp-table-cell>{val}</sp-table-cell>
						{/each}
					</sp-table-row>
				{/each}
			</sp-table-body>
		</sp-table>
	{/if}
</div>

<style>
	.container {
		min-width: 400px;
		max-width: 50vw;
	}
</style>
