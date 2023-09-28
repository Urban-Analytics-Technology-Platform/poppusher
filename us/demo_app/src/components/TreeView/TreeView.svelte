<script lang="ts">
	//	import { slide } from 'svelte/transition'
	import { selectedMetrics } from '../../stores/metrics';
	export let tree;

	let _expansionState = {};

	let expanded = _expansionState[tree.name] || false;
	const toggleExpansion = () => {
		expanded = _expansionState[tree.name] = !expanded;
	};
	$: arrowDown = expanded;
</script>

<ul>
	<!-- transition:slide -->
	<li>
		{#if tree.children && tree.children.length > 0}
			<span on:click={toggleExpansion}>
				<span class="arrow" class:arrowDown>&#x25b6</span>
				{tree.name} ({tree.id})
				<sp-checkbox
					checked={$selectedMetrics.includes(tree.id)}
					on:change={() => selectedMetrics.toggleMetric(tree.id)}
				/>
			</span>
			{#if expanded}
				{#each tree.children as child}
					<svelte:self tree={child} />
				{/each}
			{/if}
		{:else}
			<span>
				<span class="no-arrow" />
				<sp-checkbox
					checked={$selectedMetrics.includes(tree.id)}
					on:change={() => selectedMetrics.toggleMetric(tree.id)}
					>{tree.name} ({tree.id})</sp-checkbox
				>
			</span>
		{/if}
	</li>
</ul>

<style>
	ul {
		margin: 0;
		list-style: none;
		padding-left: 1.2rem;
		user-select: none;
	}
	.no-arrow {
		padding-left: 1rem;
	}
	.arrow {
		cursor: pointer;
		display: inline-block;
		/* transition: transform 200ms; */
	}
	.arrowDown {
		transform: rotate(90deg);
	}
</style>
