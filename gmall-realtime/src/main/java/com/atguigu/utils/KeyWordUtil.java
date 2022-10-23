package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeyWordUtil {

    public static List<String> splitKeyWord(String keyWord) throws IOException {

        //创建集合用于存放切分后的数据
        ArrayList<String> list = new ArrayList<>();

        //创建IK分词对象 is_smart ,ik_max_word 控制切分粒度
        StringReader reader = new StringReader(keyWord);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        //循环取出切分好的词
        Lexeme next = ikSegmenter.next();

        while (next!=null){
            String word = next.getLexemeText();
            list.add(word);

            next = ikSegmenter.next();
        }

        //最终返回集合
        return list;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(splitKeyWord("尚硅谷大数据FLink"));
    }
}
