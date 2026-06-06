<template>
  <div class="mermaid-block">
    <div v-if="error" class="mermaid-block__error">
      <p>{{ error }}</p>
      <pre>{{ decodedCode }}</pre>
    </div>
    <div v-else ref="container" class="mermaid-block__svg" />
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useData } from 'vitepress'

const props = defineProps<{
  id: string
  code: string
}>()

const { isDark } = useData()
const container = ref<HTMLElement | null>(null)
const error = ref('')
const decodedCode = computed(() => decodeURIComponent(props.code))
let renderVersion = 0

async function renderDiagram() {
  if (!container.value) {
    return
  }

  const currentVersion = ++renderVersion
  error.value = ''

  try {
    const mermaid = (await import('mermaid')).default

    mermaid.initialize({
      securityLevel: 'strict',
      startOnLoad: false,
      theme: isDark.value ? 'dark' : 'default',
    })

    const { svg } = await mermaid.render(`${props.id}-${currentVersion}`, decodedCode.value)

    if (currentVersion === renderVersion && container.value) {
      container.value.innerHTML = svg
    }
  } catch (renderError) {
    if (currentVersion !== renderVersion || !container.value) {
      return
    }

    container.value.innerHTML = ''
    error.value = renderError instanceof Error ? renderError.message : String(renderError)
  }
}

onMounted(() => {
  void renderDiagram()
})

onBeforeUnmount(() => {
  renderVersion += 1

  if (container.value) {
    container.value.innerHTML = ''
  }
})

watch(
  () => [props.code, isDark.value],
  () => {
    void nextTick(renderDiagram)
  },
)
</script>
