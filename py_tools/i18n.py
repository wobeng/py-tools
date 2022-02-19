#!/usr/bin/env python
# -*- coding:utf-8 -*-
import gettext

import os


class Localization:
    def __init__(
        self, localedir="locale", default_lang="en", domain="messages"
    ):
        self.domain = domain
        self.localedir = localedir
        self.active_lang = default_lang
        self.default_lang = default_lang
        self.locales, self.all_translations = self.load_translations(
            self.localedir, self.default_lang, self.domain
        )

    def __call__(self, localedir=None, default_lang=None, domain=None):
        localedir = localedir or self.localedir
        default_lang = default_lang or self.default_lang
        domain = domain or self.domain
        self.locales, self.all_translations = self.load_translations(
            localedir, default_lang, domain
        )

    @staticmethod
    def load_translations(localedir, default_lang, domain):
        locales = []
        all_translations = {}

        # find out all supported locales in locale directory
        for dirpath, dirnames, filenames in os.walk(localedir):
            for dirname in dirnames:
                locales.append(dirname)
            break

        for locale in locales:
            try:
                all_translations[locale] = gettext.translation(
                    domain, localedir, [locale]
                )
            except FileNotFoundError:
                all_translations[locale] = gettext.translation(
                    domain, localedir, [default_lang]
                )

        return locales, all_translations

    def set_locale(self, lang):
        if lang in self.locales:
            self.active_lang = lang

    def gettext(self, message):
        return self.all_translations[self.active_lang].gettext(message)

    def ugettext(self, message):
        return self.all_translations[self.active_lang].gettext(message)

    def ngettext(self, singular, plural, n):
        return self.all_translations[self.active_lang].ngettext(
            singular, plural, n
        )

    def ungettext(self, singular, plural, n):
        return self.all_translations[self.active_lang].ungettext(
            singular, plural, n
        )


# init localization and make _ available
i18n = Localization()
_ = i18n.gettext
