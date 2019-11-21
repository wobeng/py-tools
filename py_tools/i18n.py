#!/usr/bin/env python
# -*- coding:utf-8 -*-
import gettext

import os


class Localization:
    def __init__(self, localedir='locale', default_lang='en', domain='messages'):

        self.localedir = os.path.join(os.getcwd(), 'locale')
        self.lang = default_lang

        # find out all supported locales in locale directory
        self.locales = []
        for dirpath, dirnames, filenames in os.walk(localedir):
            for dirname in dirnames:
                self.locales.append(dirname)
            break

        self.AllTranslations = {}
        for locale in self.locales:
            try:
                self.AllTranslations[locale] = gettext.translation(domain, localedir, [locale])
            except FileNotFoundError:
                self.AllTranslations[locale] = gettext.translation(domain, localedir, [default_lang])

    def set_locale(self, lang):
        if lang in self.locales:
            self.lang = lang

    def gettext(self, message):
        return self.AllTranslations[self.lang].gettext(message)

    def ugettext(self, message):
        return self.AllTranslations[self.lang].gettext(message)

    def ngettext(self, singular, plural, n):
        return self.AllTranslations[self.lang].ngettext(singular, plural, n)

    def ungettext(self, singular, plural, n):
        return self.AllTranslations[self.lang].ungettext(singular, plural, n)


# init localization and make _ available
i18n = Localization()
_ = i18n.gettext
