<script lang="ts">
	import Layout from '../../routes/+layout.svelte';
	import VirtualTable from 'svelte-virtual-table';

	import { selectedMetrics, metricValues, selectedMetric } from '../../stores/metrics';
	import { html } from 'lit-html';

	$: metricValues.setIds($selectedMetrics);

	let metricDict: Record<number, Record<string, number>> = {};
	let table;

	$: {
		if ($metricValues.value) {
			for (const rowNo in $metricValues.value) {
				metricDict[`${rowNo}`] = $metricValues.value[rowNo];
			}
		}
		console.log('Metric Dict ', metricDict);
	}
	// $: metricValuesDict = $metricValues.value?.reduce(
	// 	(dict, row, index) => ({ ...dict, [index]: row }),
	// 	{}
	// );

	// $: console.log('Dict', metricValuesDict);
	$: console.log('metric values ', $metricValues);

	$: {
		if (table) {
			table.renderItem = (item, index) => {
				console.log('RENDERING');
				const cell1 = document.createElement('sp-table-cell');
				const cell2 = document.createElement('sp-table-cell');
				cell1.textContent = `Row Item Alpha`;
				cell2.textContent = `Row Item Beta`;
				return [cell1];
			};
			table.items = metricDict;
			console.log(table);
		}
	}

	function renderRow(item, index) {
		console.log('rendering ');
		return html` ${Object.values(row).map((r) => html`<sp-table-cell>${r}</sp-table-cell>`)} `;
	}
</script>

<div class="container">
	{#if $selectedMetrics.length === 0}
		<p>Select some metrics to load</p>
	{:else if $metricValues.loading}
		<p>Loading</p>
	{:else if $metricValues.value.error}
		<p>Something went wrong {$metricValues.value.error}</p>
	{:else}
		<div class="table">
			<VirtualTable items={$metricValues.value}>
				<tr slot="thead" role="row">
					<th data-sort="GEOID">GeoID</th>
					{#each $selectedMetrics as metricId}
						<th data-stort={metricId}>
							<button on:click={() => ($selectedMetric = metricId.split('_').join('_E'))}>
								{metricId}
							</button>
						</th>
					{/each}
				</tr>
				<tr slot="tbody" role="row" let:item>
					{#each Object.values(item) as val}
						<td>
							<p>{val}</p>
						</td>
					{/each}
				</tr>
			</VirtualTable>
		</div>
	{/if}
</div>

<style>
	.container {
		min-width: 400px;
		max-width: 50vw;
	}
	.table {
		height: 400px;
	}
</style>
