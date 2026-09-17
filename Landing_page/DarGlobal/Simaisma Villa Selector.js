

(function () {
  function initialize() {
    document.querySelectorAll('.svilla').forEach(function (root, index) {
      if (root.dataset.initialized === 'true') return;
      root.dataset.initialized = 'true';

      var uid = 'svilla-' + Date.now().toString(36) + '-' + index;
      var one = function (selector) {
        return root.querySelector(selector);
      };

      var tabs = Array.from(root.querySelectorAll('.svilla__tab'));
      var layouts = Array.from(root.querySelectorAll('.svilla__variant'));
      var templates = Array.from(root.querySelectorAll('template[data-villa]'));
      var panel = one('.svilla__panel');
      var preview = one('.svilla__preview');
      var enquiry = one('.svilla__enquire');
      var dialog = one('.svilla__dialog');
      var viewport = one('.svilla__viewport');
      var plan = one('.svilla__plan');

      if (!panel || !preview || !enquiry || !dialog || !viewport || !plan || !templates.length) return;

      // Route enquiries through the existing WaTrck CTA click handler.
      enquiry.classList.add('custom_action_cta');
      root.dataset.enquiryReady = 'true';

      var selected;
      var floors = [];
      var remembered = {};
      var zoom = 1;
      var panX = 0;
      var panY = 0;
      var fitWidth = 0;
      var fitHeight = 0;
      var dragging = null;
      var previousOverflow = '';

      panel.id = uid + '-panel';
      dialog.setAttribute('aria-labelledby', uid + '-viewer-title');
      one('.svilla__viewer-title').id = uid + '-viewer-title';
      viewport.id = uid + '-plan';

      tabs.forEach(function (tab, i) {
        tab.id = uid + '-tab-' + i;
        tab.setAttribute('aria-controls', panel.id);
      });

      function hasSource(img) {
        return !!(img && img.getAttribute('src') && img.getAttribute('src').trim());
      }

      function textFrom(template, selector) {
        var element = template.content.querySelector(selector);
        return element ? element.textContent.trim() : '';
      }

      function showAsset(source, target, initial) {
        target.replaceChildren();
        if (!hasSource(source)) return;

        var img = source.cloneNode(true);
        img.loading = initial ? 'eager' : 'lazy';
        img.decoding = 'async';
        img.setAttribute('fetchpriority', initial ? 'high' : 'auto');
        img.addEventListener('error', function () {
          img.hidden = true;
        });
        target.appendChild(img);
      }

      function select(template, initial) {
        if (!template) return;

        selected = template;
        remembered[template.dataset.style] = template.dataset.villa;

        tabs.forEach(function (tab) {
          var active = tab.dataset.style === template.dataset.style;
          tab.setAttribute('aria-selected', String(active));
          tab.tabIndex = active ? 0 : -1;
          if (active) panel.setAttribute('aria-labelledby', tab.id);
        });

        layouts.forEach(function (button) {
          button.hidden = button.dataset.style !== template.dataset.style;
          button.setAttribute(
            'aria-pressed',
            String(button.dataset.layout === template.dataset.villa)
          );
        });

        one('.svilla__identity').textContent = textFrom(template, '[data-identity]');
        one('.svilla__title').textContent = textFrom(template, '[data-title]');
        one('.svilla__description').textContent = textFrom(template, '[data-description]');
        one('.svilla__specs [data-area]').textContent = textFrom(template, '[data-area]');
        one('.svilla__specs [data-price]').textContent = textFrom(template, '[data-price]');

        showAsset(
          template.content.querySelector('[data-render]'),
          one('.svilla__render'),
          initial
        );

        showAsset(
          template.content.querySelector('[data-preview]'),
          one('.svilla__thumbnail'),
          false
        );

        var plansTemplate = template.content.querySelector('template[data-plans]');

        floors = plansTemplate
          ? Array.from(plansTemplate.content.querySelectorAll('[data-floor]'))
          : [];

        var combined = floors.find(function (floor) {
          return floor.dataset.floor === 'combined' &&
            hasSource(floor.querySelector('img'));
        });

        floors = combined
          ? [combined]
          : floors.filter(function (floor) {
              return floor.dataset.floor !== 'combined';
            });

        preview.disabled = !floors.some(function (floor) {
          return hasSource(floor.querySelector('img'));
        });

        enquiry.disabled = root.dataset.enquiryReady !== 'true';
        enquiry.dataset.enquiryContext = template.dataset.enquiryContext;
        enquiry.dataset.waMessage =
          'Hello, I would like more information about ' +
          template.dataset.enquiryContext.replace(/\s*\|\s*/g, ', ') +
          '. Please share availability and pricing.';
        enquiry.dataset.waCtaId =
          'villa_enquiry_' + template.dataset.style + '_' + template.dataset.villa;

        if (!initial) {
          one('[data-selection-status]').textContent =
            template.dataset.enquiryContext;
        }
      }

      function moveTab(event, buttons) {
        var position = buttons.indexOf(event.target);
        if (position < 0) return;

        var next;

        if (event.key === 'ArrowRight') {
          next = (position + 1) % buttons.length;
        }

        if (event.key === 'ArrowLeft') {
          next = (position - 1 + buttons.length) % buttons.length;
        }

        if (event.key === 'Home') next = 0;
        if (event.key === 'End') next = buttons.length - 1;
        if (next === undefined) return;

        event.preventDefault();
        buttons[next].focus();
        buttons[next].click();
      }

      tabs.forEach(function (tab) {
        tab.addEventListener('click', function () {
          select(
            templates.find(function (template) {
              return template.dataset.style === tab.dataset.style &&
                (!remembered[tab.dataset.style] ||
                  template.dataset.villa === remembered[tab.dataset.style]);
            }),
            false
          );
        });
      });

      one('.svilla__tabs').addEventListener('keydown', function (event) {
        moveTab(event, tabs);
      });

      layouts.forEach(function (button) {
        button.addEventListener('click', function () {
          select(
            templates.find(function (template) {
              return template.dataset.villa === button.dataset.layout;
            }),
            false
          );
        });
      });

      enquiry.addEventListener('click', function () {
        if (!selected || enquiry.disabled) return;

        root.dispatchEvent(new CustomEvent(root.dataset.enquiryEvent, {
          bubbles: true,
          detail: {
            context: selected.dataset.enquiryContext,
            style: selected.dataset.style,
            variant: selected.dataset.layoutLabel,
            builtUpArea: textFrom(selected, '[data-area]'),
            startingPrice: textFrom(selected, '[data-price]'),
            trigger: enquiry
          }
        }));
      });

      function paint() {
        var limitX = Math.max(
          0,
          (fitWidth * zoom - viewport.clientWidth) / 2
        );

        var limitY = Math.max(
          0,
          (fitHeight * zoom - viewport.clientHeight) / 2
        );

        panX = Math.max(-limitX, Math.min(limitX, panX));
        panY = Math.max(-limitY, Math.min(limitY, panY));

        plan.style.transform =
          'translate(-50%, -50%) translate(' +
          panX + 'px, ' + panY + 'px) scale(' + zoom + ')';

        viewport.dataset.zoomed = String(zoom > 1);

        one('.svilla__zoom output').textContent =
          Math.round(zoom * 100) + '%';

        one('[data-zoom="out"]').disabled = plan.hidden || zoom <= 1;
        one('[data-zoom="in"]').disabled = plan.hidden || zoom >= 4;
        one('[data-zoom="reset"]').disabled = plan.hidden;
      }

      function fit() {
        if (plan.hidden || !plan.naturalWidth || !viewport.clientWidth) return;

        var ratio = Math.min(
          (viewport.clientWidth - 24) / plan.naturalWidth,
          (viewport.clientHeight - 24) / plan.naturalHeight
        );

        fitWidth = plan.naturalWidth * ratio;
        fitHeight = plan.naturalHeight * ratio;

        plan.style.width = fitWidth + 'px';
        plan.style.height = fitHeight + 'px';

        paint();
      }

      function changeZoom(value) {
        if (plan.hidden) return;

        zoom = Math.max(1, Math.min(4, value));

        if (zoom === 1) {
          panX = 0;
          panY = 0;
        }

        paint();
      }

      function loadFloor(position) {
        zoom = 1;
        panX = 0;
        panY = 0;
        dragging = null;
        viewport.dataset.dragging = 'false';

        plan.hidden = true;
        plan.removeAttribute('src');
        plan.removeAttribute('srcset');

        one('[data-loading]').hidden = false;
        one('[data-plan-error]').hidden = true;
        viewport.setAttribute('aria-busy', 'true');

        var source = floors[position].querySelector('img');
        var buttons = Array.from(one('.svilla__floors').children);

        buttons.forEach(function (button, i) {
          button.setAttribute('aria-selected', String(i === position));
          button.tabIndex = i === position ? 0 : -1;
        });

        viewport.setAttribute('aria-labelledby', buttons[position].id);
        plan.alt = source.alt;

        paint();
        plan.src = source.getAttribute('src');
      }

      plan.addEventListener('load', function () {
        if (!plan.getAttribute('src')) return;

        one('[data-loading]').hidden = true;
        one('[data-plan-error]').hidden = true;
        viewport.setAttribute('aria-busy', 'false');
        plan.hidden = false;

        fit();
      });

      plan.addEventListener('error', function () {
        if (!plan.getAttribute('src')) return;

        plan.hidden = true;
        one('[data-loading]').hidden = true;
        one('[data-plan-error]').hidden = false;
        viewport.setAttribute('aria-busy', 'false');

        paint();
      });

      preview.addEventListener('click', function () {
        if (preview.disabled || dialog.open) return;

        one('.svilla__viewer-context').textContent =
          selected.dataset.enquiryContext;

        var controls = one('.svilla__floors');
        controls.replaceChildren();

        floors.forEach(function (floor, position) {
          var button = document.createElement('button');

          button.type = 'button';
          button.className = 'svilla__floor';
          button.id = uid + '-floor-' + position;
          button.setAttribute('role', 'tab');
          button.setAttribute('aria-controls', viewport.id);
          button.textContent = floor.querySelector('figcaption').textContent;
          button.disabled = !hasSource(floor.querySelector('img'));

          button.addEventListener('click', function () {
            loadFloor(position);
          });

          controls.appendChild(button);
        });

        previousOverflow = document.documentElement.style.overflow;

        dialog.showModal();
        document.documentElement.style.overflow = 'hidden';

        loadFloor(floors.findIndex(function (floor) {
          return hasSource(floor.querySelector('img'));
        }));

        one('.svilla__close').focus();
      });

      one('.svilla__floors').addEventListener('keydown', function (event) {
        moveTab(
          event,
          Array.from(this.querySelectorAll('button:not(:disabled)'))
        );
      });

      one('.svilla__close').addEventListener('click', function () {
        dialog.close();
      });

      dialog.addEventListener('click', function (event) {
        if (event.target === dialog) dialog.close();
      });

      dialog.addEventListener('close', function () {
        document.documentElement.style.overflow = previousOverflow;
        dragging = null;
        preview.focus({ preventScroll: true });
      });

      root.querySelectorAll('[data-zoom]').forEach(function (button) {
        button.addEventListener('click', function () {
          changeZoom(
            button.dataset.zoom === 'reset'
              ? 1
              : zoom + (button.dataset.zoom === 'in' ? 0.5 : -0.5)
          );
        });
      });

      viewport.addEventListener('keydown', function (event) {
        if (['+', '=', '-', '0'].indexOf(event.key) !== -1) {
          event.preventDefault();

          changeZoom(
            event.key === '0'
              ? 1
              : zoom + (event.key === '-' ? -0.5 : 0.5)
          );
        }

        if (zoom <= 1) return;

        var directions = {
          ArrowLeft: [40, 0],
          ArrowRight: [-40, 0],
          ArrowUp: [0, 40],
          ArrowDown: [0, -40]
        };

        if (!directions[event.key]) return;

        event.preventDefault();
        panX += directions[event.key][0];
        panY += directions[event.key][1];

        paint();
      });

      viewport.addEventListener('pointerdown', function (event) {
        if (
          zoom <= 1 ||
          plan.hidden ||
          !event.isPrimary ||
          (event.pointerType === 'mouse' && event.button !== 0)
        ) return;

        dragging = {
          id: event.pointerId,
          x: event.clientX,
          y: event.clientY
        };

        viewport.setPointerCapture(event.pointerId);
        viewport.dataset.dragging = 'true';
      });

      viewport.addEventListener('pointermove', function (event) {
        if (!dragging || dragging.id !== event.pointerId) return;

        panX += event.clientX - dragging.x;
        panY += event.clientY - dragging.y;
        dragging.x = event.clientX;
        dragging.y = event.clientY;

        paint();
      });

      function stopDrag() {
        dragging = null;
        viewport.dataset.dragging = 'false';
      }

      viewport.addEventListener('pointerup', stopDrag);
      viewport.addEventListener('pointercancel', stopDrag);
      viewport.addEventListener('lostpointercapture', stopDrag);

      if ('ResizeObserver' in window) {
        new ResizeObserver(fit).observe(viewport);
      } else {
        window.addEventListener('resize', fit);
      }

      select(templates[0], true);
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initialize, { once: true });
  } else {
    initialize();
  }
})();
