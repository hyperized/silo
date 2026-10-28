import pluginVue from 'eslint-plugin-vue'
import { defineConfigWithVueTs, vueTsConfigs } from '@vue/eslint-config-typescript'

export default defineConfigWithVueTs(
  { ignores: ['coverage/**', 'node_modules/**'] },
  pluginVue.configs['flat/recommended'],
  vueTsConfigs.strictTypeChecked,
  {
    rules: {
      'vue/multi-word-component-names': 'off',
    },
  },
)
