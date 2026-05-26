---
layout: false
title: SynapS3
titleTemplate: false
description: Redirecting to the English documentation.
head:
  - - meta
    - http-equiv: refresh
      content: 0; url=en/
---

<script setup>
import { inBrowser, withBase } from 'vitepress'

if (inBrowser) {
  window.location.replace(withBase('/en/'))
}
</script>

Redirecting to [English documentation](/en/).
